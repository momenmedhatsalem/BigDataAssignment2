import subprocess
import time
import re
import os

class WikimediaBenchmarker:
    def __init__(self, scripts):
        self.scripts = scripts
        self.results = {}

    def run_script(self, script_name):
        print(f"--- Executing {script_name} ---")
        start_time = time.time()
        
        process = subprocess.Popen(
            ['python3', script_name], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate()
        
        end_time = time.time()
        duration = end_time - start_time
        
        if process.returncode != 0:
            print(f"Error running {script_name}: {stderr}")
            return None
            
        print(f"Finished {script_name} in {duration:.2f}s\n")
        return stdout

    def parse_results(self, output):
        """
        Extracts timing and key metrics from terminal output using regex.
        Adjust the regex patterns if your teammates change their print statements.
        """
        data = {}
        # Extract Map-Reduce Times
        mr_match = re.search(r"(?:Map-Reduce|MAP REDUCE).*?:\s*([\d\.]+)s?", output, re.IGNORECASE)
        if mr_match:
            data['mr_time'] = float(mr_match.group(1))
            
        # Extract Loop/Aggregate Times
        loop_match = re.search(r"(?:Loop|Spark Loop).*?:\s*([\d\.]+)s?", output, re.IGNORECASE)
        if loop_match:
            data['loop_time'] = float(loop_match.group(1))
            
        return data

    def generate_report(self):
        report_lines = [
            "==========================================================",
            "WIKIMEDIA BIG DATA ASSIGNMENT: PERFORMANCE REPORT",
            "==========================================================\n"
        ]

        for script in self.scripts:
            output = self.run_script(script)
            if output:
                metrics = self.parse_results(output)
                report_lines.append(f"### Results for {script}")
                report_lines.append(output)
                
                if 'mr_time' in metrics and 'loop_time' in metrics:
                    mr = metrics['mr_time']
                    lp = metrics['loop_time']
                    faster = "Map-Reduce" if mr < lp else "Loop/Aggregate"
                    diff = abs(mr - lp)
                    ratio = max(mr, lp) / min(mr, lp)
                    
                    report_lines.append("--- Performance Analysis ---")
                    report_lines.append(f"Winner          : {faster}")
                    report_lines.append(f"Time Difference : {diff:.4f}s")
                    report_lines.append(f"Speedup Factor  : {ratio:.2f}x")
                report_lines.append("\n" + "-"*30 + "\n")

        with open("final_report.txt", "w") as f:
            f.write("\n".join(report_lines))
        
        print("Success! Final report generated: final_report.txt")

if __name__ == "__main__":
    teammate_scripts = [
        "query1.py",
        "query2_3.py",
        "query4_5.py"
    ]
    
    # Check if files exist before running
    missing = [f for f in teammate_scripts if not os.path.exists(f)]
    if missing:
        print(f"Error: Missing files: {missing}")
    else:
        benchmarker = WikimediaBenchmarker(teammate_scripts)
        benchmarker.generate_report()