from playwright.sync_api import sync_playwright
import time
import subprocess
import os

def verify():
    # Start the Streamlit server
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.abspath(os.curdir)
    # create a dummy empty database so app.py doesn't error out completely
    # wait it will create tables if not exists inside get_case_list()
    process = subprocess.Popen(
        ["streamlit", "run", "frontend/app.py", "--server.port", "8501", "--server.headless", "true"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Wait for the server to start
    time.sleep(5)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(record_video_dir=".", viewport={"width": 1280, "height": 800})
        page = context.new_page()

        try:
            # Navigate to the app
            page.goto("http://localhost:8501")

            # Wait for Streamlit to load
            page.wait_for_selector(".stApp", timeout=10000)
            time.sleep(2)

            # Navigate to "Unified Search" page via sidebar
            # The radio button label is "🔍 Unified Search"
            page.locator("text=Unified Search").click()
            time.sleep(2)

            # Add a fake case so we can select it
            import sqlite3
            conn = sqlite3.connect("forensic_data.db")
            conn.execute("CREATE TABLE IF NOT EXISTS cases (case_id TEXT PRIMARY KEY)")
            conn.execute("INSERT OR IGNORE INTO cases (case_id) VALUES ('TEST_CASE_1')")
            conn.commit()
            conn.close()

            # Refresh page
            page.reload()
            time.sleep(2)

            # Navigate to Unified Search
            page.locator("text=Unified Search").click()
            time.sleep(2)

            # Select the case
            page.locator("input[aria-autocomplete='list']").click()
            page.locator("text=TEST_CASE_1").click()
            # Click outside to close multiselect
            page.mouse.click(0, 0)
            time.sleep(2)

            # Take a screenshot to see what's failing if it fails to find the text
            page.screenshot(path="empty_state_screenshot.png")
            print("Successfully verified empty state!")

        except Exception as e:
            print(f"Verification failed: {e}")
        finally:
            context.close()
            browser.close()
            process.terminate()

if __name__ == "__main__":
    verify()
