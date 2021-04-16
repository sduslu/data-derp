# Development Environment
In order reduce the amount of effort of setting up development environments and fixing versioning between machines, a dev-container is provided which works with the Remote-Container functionality in Visual Studio code.

## Prerequisites
* [Docker](https://www.docker.com/products/docker-desktop)
* [Visual Studio](https://code.visualstudio.com/download)

## Install
1. Build dev-container: `./go build-dev-container`
2. In VS Code's top menu: **View** > **Extensions**. Install the following Extensions on VS Code:
    - **Remote - Containers** (author: Microsoft)
    - **Python** (author: Microsoft)
    - **Jupyter** (author: Microsoft)
    - **Pylance** (author: Microsoft) 
3. In VS Code's top menu: **View** > **Command Palette...**. Type **Remote-Containers: Open Folder in Container**
4. Navigate to the root of this repository (**twdu-germany**), then click 'Open'. 
5. Open the **Extensions** pane (bottom-most chicklet on the left) and check that Python, Jupyter, and Pylance all have been installed in your **Dev Container**. If not, click **Install** on each of them.


## Usage
1. Once the Python extension is installed: for any .py file, you should be able to click on the green ▷ button towards the top right corner to run your script
2. You can also run/create interactive notebooks (the file name just needs to end with `.ipynb`). You'll even have IntelliSense thanks to Pylance!
3. If you *really* prefer the original Jupyter:
    - type `jupyter lab --allow-root` into the VS Code terminal
    - the terminal should now output a few URLs on port 8888, each containing a token
    - **⌘ + click** any of those URLs (try both if needed)
