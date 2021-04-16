# Setup Instructions 
1. Clone this Git repository, then switch to the **pawarit** branch (temporary)
2. Install Docker Desktop (https://www.docker.com/products/docker-desktop)
3. **Recommended:** In your macOS terminal, run `docker pull pawaritl/twdu-germany:latest` (while it's downloading, continue Steps 4 - 6)
4. Install VS Code (https://code.visualstudio.com/download)
5. In VS Code, click **View** in the top menu, then click **Extensions**. Install the following Extensions on VS Code:
    - **Remote - Containers** (author: Microsoft)
    - **Python** (author: Microsoft)
    - **Jupyter** (author: Microsoft)
    - **Pylance** (author: Microsoft) 
6. In VS Code, click **View** in the top menu. Click **Command Palette...**, then type the following: **Remote-Containers: Open Folder in Container**
7. Navigate to the root of this Git repository (**twdu-germany**), then click 'Open'. 
8. Open the **Extensions** pane and check that Python, Jupyter, and Pylance all have been installed in your **Dev Container**. If not, click **Install** on each of them

You should be good to go!

# Usage
1. Once the Python extension is installed: for any .py file, you should be able to click on the green ▷ button towards the top right corner to run your script
2. You can also run/create interactive notebooks (the file name just needs to end with `.ipynb`). You'll even have IntelliSense thanks to Pylance!
3. If you *really* prefer the original Jupyter:
    - type `jupyter lab --allow-root` into the VS Code terminal
    - the terminal should now output a few URLs on port 8888, each containing a token
    - **⌘ + click** any of those URLs (try both if needed)
