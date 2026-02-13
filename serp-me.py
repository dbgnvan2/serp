import tkinter as tk
from tkinter import ttk, scrolledtext
import subprocess
import sys
import threading
import os


class SerpLauncherApp:
    def __init__(self, root):
        self.root = root
        self.root.title("SERP Intelligence Launcher")
        self.root.geometry("800x650")

        # Styles
        style = ttk.Style()
        style.configure("TButton", padding=6)
        style.configure("TLabel", font=("Helvetica", 10))
        style.configure("Header.TLabel", font=("Helvetica", 16, "bold"))

        # Header
        header_frame = ttk.Frame(root)
        header_frame.pack(pady=15, fill="x", padx=20)

        title_label = ttk.Label(
            header_frame, text="SERP Intelligence Tool", style="Header.TLabel")
        title_label.pack()

        subtitle_label = ttk.Label(
            header_frame, text="Bridge Strategy & Market Analysis Pipeline")
        subtitle_label.pack()

        # Main Content Area (Split into Selection and Description)
        content_frame = ttk.Frame(root)
        content_frame.pack(pady=10, fill="both", expand=True, padx=20)

        # Left Side: List of Scripts
        list_frame = ttk.LabelFrame(content_frame, text="Available Scripts")
        list_frame.pack(side="left", fill="both", expand=True, padx=(0, 10))

        self.script_listbox = tk.Listbox(
            list_frame, height=10, font=("Courier", 12), activestyle="none")
        self.script_listbox.pack(
            side="left", fill="both", expand=True, padx=5, pady=5)
        self.script_listbox.bind('<<ListboxSelect>>', self.on_select)

        scrollbar = ttk.Scrollbar(
            list_frame, orient="vertical", command=self.script_listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.script_listbox.config(yscrollcommand=scrollbar.set)

        # Right Side: Description & Context
        desc_frame = ttk.LabelFrame(content_frame, text="Context & Usage")
        desc_frame.pack(side="right", fill="both", expand=True)

        self.desc_text = tk.Text(desc_frame, wrap="word", height=10,
                                 width=35, bg="#f9f9f9", state="disabled", font=("Helvetica", 11))
        self.desc_text.pack(fill="both", expand=True, padx=10, pady=10)

        # Actions Configuration
        self.scripts = [
            {
                "label": "1. Run Full Pipeline (Daily)",
                "file": "run_pipeline.py",
                "args": [],
                "desc": (
                    "WHEN: Run this once per day or weekly.\n\n"
                    "WHY: This is the 'Daily Driver'. It performs the full audit:\n"
                    "  - Fetches SERP data (Google, Maps, AI)\n"
                    "  - Enriches data (HTML parsing, Entity Classification)\n"
                    "  - Stores history in SQLite\n"
                    "  - Generates Excel/Markdown reports\n"
                    "  - Validates data integrity"
                )
            },
            {
                "label": "2. List Content Opportunities",
                "file": "generate_content_brief.py",
                "args": ["--json", "market_analysis_v2.json", "--list"],
                "desc": (
                    "WHEN: Run when you are ready to write content.\n\n"
                    "WHY: The 'Strategist'. Lists strategic recommendations found in the analysis (e.g., 'The Medical Model Trap').\n\n"
                    "NOTE: To generate a specific brief, run this script from the command line with --index <N>."
                )
            },
            {
                "label": "3. List Volatility Keywords",
                "file": "visualize_volatility.py",
                "args": ["--list"],
                "desc": (
                    "WHEN: Run after accumulating a few days of data.\n\n"
                    "WHY: The 'Analyst'. Lists keywords available for historical tracking.\n\n"
                    "NOTE: To generate a chart, run from command line: python visualize_volatility.py --keyword 'Your Keyword'"
                )
            },
            {
                "label": "4. Export History to CSV",
                "file": "export_history.py",
                "args": [],
                "desc": (
                    "WHEN: Run monthly or when external analysis is needed.\n\n"
                    "WHY: Dumps the entire SQLite database (runs, serp_results, features) into CSV files in the 'exports/' folder."
                )
            },
            {
                "label": "5. Verify Database",
                "file": "verify_enrichment.py",
                "args": [],
                "desc": (
                    "WHEN: Run if you suspect data issues.\n\n"
                    "WHY: Checks the SQLite database to confirm that enrichment data (URL features, Domain features) is being correctly populated."
                )
            }
        ]

        for s in self.scripts:
            self.script_listbox.insert(tk.END, s["label"])

        # Control Buttons
        btn_frame = ttk.Frame(root)
        btn_frame.pack(pady=10, fill="x", padx=20)

        self.run_btn = ttk.Button(
            btn_frame, text="Run Selected Script", command=self.run_script, state="disabled")
        self.run_btn.pack(side="right", padx=5)

        ttk.Button(btn_frame, text="Clear Log",
                   command=self.clear_log).pack(side="right", padx=5)

        # Output Log
        log_frame = ttk.LabelFrame(root, text="Execution Log")
        log_frame.pack(pady=(0, 20), fill="both", expand=True, padx=20)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=12, state="disabled", bg="#1e1e1e", fg="#00ff00", font=("Consolas", 10))
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)

    def on_select(self, event):
        selection = self.script_listbox.curselection()
        if selection:
            index = selection[0]
            desc = self.scripts[index]["desc"]
            self.update_desc(desc)
            self.run_btn.config(state="normal")
        else:
            self.update_desc("")
            self.run_btn.config(state="disabled")

    def update_desc(self, text):
        self.desc_text.config(state="normal")
        self.desc_text.delete("1.0", tk.END)
        self.desc_text.insert(tk.END, text)
        self.desc_text.config(state="disabled")

    def run_script(self):
        selection = self.script_listbox.curselection()
        if not selection:
            return

        script_info = self.scripts[selection[0]]
        cmd = [sys.executable, script_info["file"]] + script_info["args"]

        self.log(f"> Executing: {' '.join(cmd)}\n")
        self.run_btn.config(state="disabled")

        threading.Thread(target=self.execute_thread,
                         args=(cmd,), daemon=True).start()

    def execute_thread(self, cmd):
        try:
            # Use Popen to capture output in real-time
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=os.getcwd()  # Ensure running in correct directory
            )

            for line in process.stdout:
                self.root.after(0, self.log, line)

            process.wait()
            self.root.after(
                0, self.log, f"\n[Process finished with exit code {process.returncode}]\n" + "-"*60 + "\n")

        except Exception as e:
            self.root.after(0, self.log, f"\n[Error starting process: {e}]\n")
        finally:
            self.root.after(0, lambda: self.run_btn.config(state="normal"))

    def log(self, message):
        self.log_text.config(state="normal")
        self.log_text.insert(tk.END, message)
        self.log_text.see(tk.END)
        self.log_text.config(state="disabled")

    def clear_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.config(state="disabled")


if __name__ == "__main__":
    root = tk.Tk()
    app = SerpLauncherApp(root)
    root.mainloop()
