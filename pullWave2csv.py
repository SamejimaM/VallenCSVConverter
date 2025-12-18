import sys
import os
import threading
import shutil
import tempfile
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path
import pandas as pd
import vallenae as vae

class AEConverterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Vallen CSV Converter")
        self.root.geometry("600x480")

        # --- 変数 ---
        self.pridb_path_var = tk.StringVar()
        self.output_dir_var = tk.StringVar()
        self.status_tradb = tk.StringVar(value="tradb: 未確認")
        self.status_trfdb = tk.StringVar(value="trfdb: 未確認")
        self.progress_var = tk.DoubleVar()
        self.status_msg = tk.StringVar(value="待機中...")
        self.is_running = False

        # --- GUIレイアウト ---
        self.create_widgets()

    def create_widgets(self):
        # 1. 入力ファイル選択エリア
        input_frame = tk.LabelFrame(self.root, text="入力設定", padx=10, pady=10)
        input_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(input_frame, text="pridbファイルを選択:").pack(anchor="w")
        
        file_frame = tk.Frame(input_frame)
        file_frame.pack(fill="x", pady=5)
        
        entry_pridb = tk.Entry(file_frame, textvariable=self.pridb_path_var, state="readonly")
        entry_pridb.pack(side="left", fill="x", expand=True)
        
        btn_browse = tk.Button(file_frame, text="参照...", command=self.select_file)
        btn_browse.pack(side="right", padx=5)

        # 関連ファイルの検出状況表示
        status_frame = tk.Frame(input_frame)
        status_frame.pack(fill="x", pady=5)
        
        lbl_tradb = tk.Label(status_frame, textvariable=self.status_tradb, fg="gray")
        lbl_tradb.pack(side="left", padx=10)
        
        lbl_trfdb = tk.Label(status_frame, textvariable=self.status_trfdb, fg="gray")
        lbl_trfdb.pack(side="left", padx=10)

        # 2. 出力先選択エリア
        output_frame = tk.LabelFrame(self.root, text="出力設定", padx=10, pady=10)
        output_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(output_frame, text="出力先ディレクトリ (空欄の場合は入力元と同じ):").pack(anchor="w")
        
        out_file_frame = tk.Frame(output_frame)
        out_file_frame.pack(fill="x", pady=5)
        
        entry_out = tk.Entry(out_file_frame, textvariable=self.output_dir_var)
        entry_out.pack(side="left", fill="x", expand=True)
        
        btn_out_browse = tk.Button(out_file_frame, text="参照...", command=self.select_output_dir)
        btn_out_browse.pack(side="right", padx=5)

        # 3. 実行エリア
        action_frame = tk.Frame(self.root, padx=10, pady=10)
        action_frame.pack(fill="x", padx=10, pady=5)

        self.btn_run = tk.Button(action_frame, text="変換実行", command=self.start_processing, bg="#dddddd", height=2)
        self.btn_run.pack(fill="x", pady=5)

        # プログレスバー
        self.progress_bar = ttk.Progressbar(action_frame, orient="horizontal", length=100, mode="determinate", variable=self.progress_var)
        self.progress_bar.pack(fill="x", pady=5)

        tk.Label(action_frame, textvariable=self.status_msg).pack(anchor="w")

    def select_file(self):
        file_path = filedialog.askopenfilename(
            title="pridbファイルを選択",
            filetypes=[("PriDatabase", "*.pridb"), ("All Files", "*.*")]
        )
        if file_path:
            self.pridb_path_var.set(file_path)
            self.check_related_files(file_path)
            if not self.output_dir_var.get():
                self.output_dir_var.set(str(Path(file_path).parent))

    def select_output_dir(self):
        dir_path = filedialog.askdirectory(title="出力先フォルダを選択")
        if dir_path:
            self.output_dir_var.set(dir_path)

    def check_related_files(self, pridb_path_str):
        path = Path(pridb_path_str)
        tradb_path = path.with_suffix('.tradb')
        trfdb_path = path.with_suffix('.trfdb')

        if tradb_path.exists():
            self.status_tradb.set(f"tradb: 発見 ({tradb_path.name})")
        else:
            self.status_tradb.set("tradb: 見つかりません")

        if trfdb_path.exists():
            self.status_trfdb.set(f"trfdb: 発見 ({trfdb_path.name})")
        else:
            self.status_trfdb.set("trfdb: 見つかりません")

    def start_processing(self):
        if self.is_running:
            return

        pridb_file = self.pridb_path_var.get()
        if not pridb_file or not os.path.exists(pridb_file):
            messagebox.showerror("エラー", "有効なpridbファイルが選択されていません。")
            return

        output_dir = self.output_dir_var.get()
        if not output_dir:
            output_dir = str(Path(pridb_file).parent)

        self.is_running = True
        self.btn_run.config(state="disabled")
        self.progress_bar.config(mode="determinate") # 通常モード
        self.progress_var.set(0)
        
        thread = threading.Thread(target=self.process_logic, args=(pridb_file, output_dir))
        thread.daemon = True
        thread.start()

    def process_logic(self, pridb_file_str, output_dir_str):
        """メイン処理"""
        try:
            # NAS上のパス情報
            src_pridb_path = Path(pridb_file_str)
            src_tradb_path = src_pridb_path.with_suffix('.tradb')
            src_trfdb_path = src_pridb_path.with_suffix('.trfdb')
            final_output_base = Path(output_dir_str)

            if not src_tradb_path.exists():
                self.update_status("エラー: tradbファイルが見つかりません。")
                self.finish_process(False)
                return

            # --- 作業用の一時ディレクトリ ---
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                
                # 1. DBファイルをTempへコピー
                self.update_status("データベースを一時フォルダへコピー中...")
                
                local_pridb = temp_dir_path / src_pridb_path.name
                shutil.copy2(src_pridb_path, local_pridb)
                
                local_tradb = temp_dir_path / src_tradb_path.name
                shutil.copy2(src_tradb_path, local_tradb)
                
                local_trfdb = temp_dir_path / src_trfdb_path.name
                if src_trfdb_path.exists():
                    shutil.copy2(src_trfdb_path, local_trfdb)
                else:
                    local_trfdb = None

                # 2. 出力先の準備（Temp内）
                output_folder_name = src_pridb_path.stem + "_csv"
                local_output_root = temp_dir_path / "Output_Buffer" / output_folder_name
                
                local_wave_folder = local_output_root / "waveforms"
                local_wave_folder.mkdir(parents=True, exist_ok=True)

                # --- データベース読み込み ---
                self.update_status("データ読み込み中...")
                
                with vae.io.TraDatabase(str(local_tradb)) as tradb:
                    sql_query = "SELECT TRAI, Time, Chan, Samples, SampleRate FROM tr_data ORDER BY TRAI"
                    wave_df = pd.read_sql(sql_query, tradb.connection())
                    wave_df.rename(columns={'TRAI': 'trai'}, inplace=True)
                    globalinfo = dict(tradb.connection().execute("SELECT Key, Value FROM tr_globalinfo").fetchall())
                    timebase = int(globalinfo.get('TimeBase', 1))

                with vae.io.PriDatabase(str(local_pridb)) as pridb:
                    ae_cols = ['trai', 'channel', 'amplitude', 'duration', 'energy', 'rms', 'counts', 'rise_time']
                    ae_df = pridb.read_hits()[ae_cols]
                    wave_df = pd.merge(wave_df, ae_df, on='trai', how='inner')

                if local_trfdb and local_trfdb.exists():
                    with vae.io.TrfDatabase(str(local_trfdb)) as trfdb:
                        freq_df = trfdb.read().reset_index()
                        freq_df = freq_df[['trai', 'FFT_FoM', 'FFT_CoG']]
                        freq_df.rename(columns={'FFT_FoM': 'Peak_Freq_kHz', 'FFT_CoG': 'Centroid_Freq_kHz'}, inplace=True)
                        wave_df = pd.merge(wave_df, freq_df, on='trai', how='left')

                # --- 波形処理 (Tempへ書き込み) ---
                merged_df = wave_df
                total_waves = len(merged_df)
                master_records = []

                self.update_status(f"波形抽出中 (Local): 全{total_waves}件")

                with vae.io.TraDatabase(str(local_tradb)) as tradb:
                    for idx, row in merged_df.iterrows():
                        trai = int(row['trai'])
                        
                        if (idx+1) % 100 == 0: 
                            progress = ((idx+1) / total_waves) * 100
                            self.root.after(0, self.update_progress_bar, progress)
                            self.root.after(0, self.update_status_msg, f"波形抽出中... {idx+1}/{total_waves}")

                        try:
                            y, t = tradb.read_wave(trai)
                            y_mv = y * 1e3
                            t_us = t * 1e6
                            
                            csv_filename = f"TRAI_{trai}.csv"
                            csv_path = local_wave_folder / csv_filename
                            
                            pd.DataFrame({
                                'Time_us': t_us, 
                                'Amplitude_mV': y_mv
                            }).to_csv(csv_path, index=False)

                        except ValueError:
                            continue

                        record = row.to_dict()
                        record['Filename'] = csv_filename
                        if 'Time' in record:
                            record['Calculated_Time_ns'] = (record['Time'] * timebase) / 16000
                        master_records.append(record)

                # --- マスターCSV保存 ---
                self.update_status("マスターCSV作成中...")
                master_df = pd.DataFrame(master_records)
                master_df.rename(columns={'Time': 'Time_tick', 'Calculated_Time_ns': 'Time_ns'}, inplace=True)
                cols = master_df.columns.tolist()
                priority_cols = ['trai', 'Filename', 'channel', 'Time_tick', 'Time_ns', 'amplitude', 'energy', 'duration', 'counts', 'rise_time', 'rms','Samples', 'SampleRate', 'Peak_Freq_kHz', 'Centroid_Freq_kHz']
                new_order = [c for c in priority_cols if c in cols]
                master_df = master_df[new_order]
                
                master_csv_path = local_output_root / f"{src_pridb_path.stem}_MasterSummary.csv"
                master_df.to_csv(master_csv_path, index=False)

                # --- NASへの一括転送 (ここを高速化) ---
                final_dest_folder = final_output_base / output_folder_name
                
                # プログレスバーを「往復アニメーション」に切り替え
                self.root.after(0, self.set_indeterminate_mode)
                self.update_status("出力ディレクトリへ一括転送中...")
                
                if final_dest_folder.exists():
                     self.update_status("注意: 出力先が存在します。上書き転送中...")
                
                # copytreeはOSレベルで効率的にコピーを行う
                shutil.copytree(local_output_root, final_dest_folder, dirs_exist_ok=True)

            self.update_status("完了しました！")
            self.finish_process(True)

        except Exception as e:
            self.update_status(f"エラー発生: {str(e)}")
            print(e)
            self.finish_process(False)

    # --- UI更新用 ---
    def set_indeterminate_mode(self):
        """プログレスバーを不確定（往復）モードにする"""
        self.progress_bar.config(mode="indeterminate")
        self.progress_bar.start(10) # 10msごとに動く

    def update_status(self, msg):
        self.root.after(0, self.update_status_msg, msg)

    def update_status_msg(self, msg):
        self.status_msg.set(msg)

    def update_progress_bar(self, val):
        self.progress_var.set(val)

    def finish_process(self, success):
        self.is_running = False
        self.root.after(0, self.reset_ui, success)

    def reset_ui(self, success):
        self.progress_bar.stop() # アニメーション停止
        self.progress_bar.config(mode="determinate") # 通常モードに戻す
        self.btn_run.config(state="normal")
        if success:
            self.progress_var.set(100)
            messagebox.showinfo("完了", "すべての処理が完了しました。")
        else:
            self.progress_var.set(0)

if __name__ == "__main__":
    root = tk.Tk()
    app = AEConverterApp(root)
    root.mainloop()