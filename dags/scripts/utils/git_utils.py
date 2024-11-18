import subprocess

def setup_safe_directory(repo_path):
    subprocess.run(["git", "config", "--global", "--add", "safe.directory", repo_path], check=True)

def get_latest_commit_id(repo_path="."):
    setup_safe_directory(repo_path)
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_path,
        capture_output=True,
        text=True,
        check=True
    )
    return result.stdout.strip()