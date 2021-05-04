# Development Environment
In order to reduce the amount of effort of setting up development environments and fixing versioning between machines, a dev-container is provided which works with the Remote-Container functionality in Visual Studio code.

## Prerequisites
* [Docker](https://www.docker.com/products/docker-desktop)
* [Visual Studio Code](https://code.visualstudio.com/download)

## Install
1. **Important:** from the root of this Git repo, run the following command in your terminal: `./data-derp build-dev-container`
2. In VS Code's top menu: **View** > **Extensions**. Install the following Extensions on VS Code:
   - **Remote - Containers** (author: Microsoft)
   - **Python** (author: Microsoft)
   - **Jupyter** (author: Microsoft)
   - **Pylance** (author: Microsoft)
3. In VS Code's top menu: **View** > **Command Palette...**. Type **Remote-Containers: Open Folder in Container**
4. Navigate to the root of this repository (**data-derp**), then click 'Open'. 

## Executing Python Code
You have two options:
- Open any .py file and use the green Play button towards the top-right corner
- From your terminal run `python <your_path.py>`
- Create a new .ipynb file and start experimenting with an interactive notebook

## Testing
1. From a terminal **inside VS Code**, run `python -m pytest` from the root of the repository if you want to test all modules/exercises
2. Otherwise, cd into the relevant directory and run `python -m pytest`
3. You can also open any of the test_xxx.py files (e.g. `test_transformation.py`) and hit the Play button
4. To test an individual test function run `python -m pytest <path_to_test_xxx.py> -k <name_of_test_function>`

## Troubleshooting
Sometimes pytest might not detect changes in your data-ingestion and data-transformation libraries. In those cases:
    - From VS Code's top menu: **View** > **Command Palette...**. Type **Remote-Containers: Rebuild Container**

## Tips & Tricks
1. To get the full path to any file, simply right-click on it and choose **Copy Path**
2. If you have too many tabs open and want to close them, right-click on the tab and choose either **Close Others** or **Close All**
3. Once the Python extension is installed: for any .py file, you should be able to click on the green ▷ button towards the top right corner to run your script
4. You can also run/create interactive notebooks (the file name just needs to end with `.ipynb`). You'll even have IntelliSense thanks to Pylance!
5. We've set the theme for interactive notebooks to **Light Mode** for easier viewing over video calls. If you'd like to change it back to the default **Dark Mode**:
    - go to `.devcontainer/devcontainer.json` and remove the line `"jupyter.ignoreVscodeTheme": true,`
    - do the same for `.vscode/settings.json`
6. If you *really* prefer the original Jupyter:
    - type `jupyter lab --allow-root` into the VS Code terminal
    - the terminal should now output a few URLs on port 8888, each containing a token
    - **⌘ + click** any of those URLs (try both if needed)
