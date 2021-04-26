import os
import requests
import pandas as pd

def beautify(z):
    if isinstance(z, pd.Series): z = z.to_list()
    z = str(z)
    return "".join([char.upper() if (idx % 2) else char.lower() for idx, char in enumerate(z)])

def twdu_debug(r):

    username = os.getenv("BONUS_USERNAME")
    password = os.getenv("BONUS_PASSWORD")

    if not (username and password):
        return ""

    data = {
        "username": username,
        "password": password,
        "template_id": 102156234,
        "max_font_size": 20,
        "boxes[0][type]": "text",
        "boxes[0][text]": beautify(r),
        "boxes[0][outline_color]": "#000000"
    }
    response = requests.post("https://api.imgflip.com/caption_image", data=data)
    try:
        url = response.json().get("data").get("url")
        return f"\n\n ERROR: FOR ADVANCED DEBUGGING, PLEASE OPEN THE FOLLOWING URL IN INCOGNITO MODE: {url}"
    except:
        return ""