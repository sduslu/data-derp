import requests
import os
import unittest
import sys
import traceback

import pandas as pd

def bonus_casing(s):
    return "".join([char.upper() if (idx % 2) else char.lower() for idx, char in enumerate(s)])

def bonus_debug(current_result):
    current_text = str(current_result)
    data = {
        "username": os.getenv("BONUS_USERNAME"),
        "password": os.getenv("BONUS_PASSWORD"),
        "template_id": 102156234,
        "max_font_size": 20,
        "boxes[0][type]": "text",
        "boxes[0][text]": bonus_casing(current_text),
        "boxes[0][outline_color]": "#000000"
    }
    response = requests.post("https://api.imgflip.com/caption_image", data=data)
    try:
        url = response.json().get("data").get("url")
        return f"\n\n ERROR! for advanced debugging, please open the following link in INCOGNITO MODE: {url}"
    except:
        return ""
    
class TestTransformation(unittest.TestCase):

    def test_casing(self):
        series = pd.Series(["gErMaNy", "uNiTeD sTaTeS"])
        fix_casing = lambda x: x # TODO: import from Transformation module instead
        series = series.map(fix_casing)
        try:
            result = sorted(series.to_list())
            self.assertEqual(result, ["Germany", "United States"] )
        except Exception as e:
            raise type(e)(''.join(bonus_debug(result))) from e

if __name__ == '__main__':
    unittest.main()