import unittest
import sys
import traceback

import pandas as pd
from bonus import debug
    
class TestTransformation(unittest.TestCase):

    def test_casing(self):
        original = pd.Series(["gErMaNy", "uNiTeD sTaTeS"])
        fix_casing = lambda x: x # TODO: import from Transformation module instead
        fixed = original.map(fix_casing)
        try:
            result = sorted(fixed.to_list())
            self.assertEqual(result, ["Germany", "United States"])
        except Exception as e:
            raise type(e)(''.join(debug(original))) from e

if __name__ == '__main__':
    unittest.main()