from DashWebApp import *

if __name__ == "__main__":

    if sys.platform == "win32":
        chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
        webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(chrome_path))
        webbrowser.get("chrome").open("http://127.0.0.1:8050/")
    elif sys.platform == "darwin":
        webbrowser.get("chrome").open("http://127.0.0.1:8050/", new=1)

    # Start Dash app
    app.run_server(threaded=False, debug=False, port=8050)