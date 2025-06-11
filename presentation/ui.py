import tkinter as tk
from tkinter import ttk, messagebox
import os

from transformations.runner import TransformationRunner


class DatabaseGUI:

    def __init__(self, root, runner: TransformationRunner):
        self._runner = runner

        self.root = root
        self.root.title("Database Transformation Tool")
        self.root.geometry("600x400")
        self.root.resizable(False, False)

        style = ttk.Style()
        style.configure("TLabel", padding=5, font=("Helvetica", 10))
        style.configure("TButton", padding=5, font=("Helvetica", 10))
        style.configure("TEntry", padding=5)

        
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        
        ttk.Label(main_frame, text="SQLite Database Path:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.db_path = ttk.Entry(main_frame, width=50)
        self.db_path.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_db).grid(row=1, column=1, padx=5)

        
        ttk.Label(main_frame, text="MongoDB URL:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.mongo_url = ttk.Entry(main_frame, width=50)
        self.mongo_url.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=5)

        
        ttk.Label(main_frame, text="Transformation Mode:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.transformation_modes = runner.modes()
        self.mode_selector = ttk.Combobox(main_frame, values=self.transformation_modes, state="readonly", width=47)
        self.mode_selector.grid(row=5, column=0, sticky=(tk.W, tk.E), pady=10)
        self.mode_selector.current(0)  

        
        ttk.Button(main_frame, text="Submit", command=self.submit).grid(row=7, column=0, pady=20)

        
        main_frame.columnconfigure(0, weight=1)

    def browse_db(self):
        from tkinter import filedialog
        path = filedialog.askopenfilename(filetypes=[("SQLite Database", "*.db")])
        if path:
            self.db_path.delete(0, tk.END)
            self.db_path.insert(0, path)

    def submit(self):
        db_path = self.db_path.get()
        mongo_url = self.mongo_url.get()
        mode = self.mode_selector.get()

        if not db_path or not os.path.exists(db_path):
            messagebox.showerror("Error", "Invalid SQLite database path")
            return
        if not mongo_url:
            messagebox.showerror("Error", "MongoDB URL is required")
            return
        if not mode:
            messagebox.showerror("Error", "Please select a transformation mode")
            return

        try:
            self._runner.configure_mongo_client(mongo_url)
        except Exception as e:
            messagebox.showerror("Error", f"Error connecting to MongoDB: {e}")
            return

        try:
            self._runner.run_transformation(mode, db_path)
        except Exception as e:
            messagebox.showerror("Error", f"Error running transformation: {e}")
            return

        messagebox.showinfo("Success", "Configuration submitted successfully!")
