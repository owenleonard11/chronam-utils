from os import walk, path, makedirs
from PIL import Image
from json import load
from typing import Union

class ChronAmJP2Clipper:
    """Uses JSON files produced using the `ChronAmXMLProcessor` class to extract clippings from JP2 images.
    
    Attributes:
        files (list[str]) : a list of truncated filepaths for which both .json and .jp2 extensions are present.
    """

    def __init__(self, data_dir: str) -> None:
        """Initializes a `ChronAmJP2Clipper` from a data directory by scanning for JP2 and corresponding JSON files."""
        self.files: list[str] = []
        for root, _, files in walk(data_dir):
            for file in files:
                if file.endswith('jp2'):
                    path_no_ext = path.splitext(path.join(root, file))[0]
                    if not path.exists(path_no_ext + '.json'):
                        print(f'WARNING: no corresponding JSON file found for JP2 file {path_no_ext}.jp2.')
                        continue
                    self.files.append(path_no_ext)
    
    @staticmethod
    def get_box(dic: dict, ratio_w: float, ratio_h: float) -> tuple[int, int, int, int]:
        """Takes a dictionary from a JSON file generated using a `ChronAmXMLProcessor` and returns a bounding box for the corresponding JP2."""
        left, right = dic['left'] / ratio_w, dic['right'] / ratio_w
        upper, lower = dic['upper'] / ratio_h, dic['lower'] / ratio_h
        return left, upper, right, lower

    def clip(self, filepath: str, level: str) -> tuple[int, str]:
        """Clips PNG images from the specified JP2 file at the specified level. PNG images are stored in a 'clippings-<level>' subirectory in the same directory as the provided `filepath`.
        
        Arguments:
            path  (str) : a path without a file extension; both .json and .jp2 extensions must be present.
            level (str) : the level of granularity at which to clip; one of "block," "line," or "word".

        Returns:
            _ (tuple[int, str]) : a tuple containing the number of clippings saved and the directory they were saved to.
        """

        for ext in ('json', 'jp2'):
            if not path.exists(f'{filepath}.{ext}'):
                raise FileNotFoundError(f'{ext} file not found at {filepath}.{ext}')
        
        if level not in ('block', 'line', 'word'):
            raise ValueError('argument level must be one of "block," "line," or "word".')
        
        clippings_dir = path.join(path.dirname(filepath), f'clippings-{level}')
        if not path.exists(clippings_dir):
            makedirs(clippings_dir)
        
        with open(f'{filepath}.json', 'r') as fp:
            page_dict: dict = load(fp)

        jp2 = Image.open(f'{filepath}.jp2')
        jp2_width, jp2_height = jp2.size
        ratio_w, ratio_h = page_dict['width'] / jp2_width, page_dict['height'] / jp2_height

        clipped = 0
        for block_id, block_dict in page_dict.items():
            # skip the "height" and "width" entries
            if type(block_dict) is float:
                continue
            if level == 'block': 
                jp2.crop(ChronAmJP2Clipper.get_box(block_dict, ratio_w, ratio_h)).save(path.join(clippings_dir, f'{block_id}.png'))
                clipped += 1

            else:
                for line_id, line_dict in block_dict.items():
                    if type(line_dict) is float:
                        continue
                    if level == 'line':
                        jp2.crop(ChronAmJP2Clipper.get_box(line_dict, ratio_w, ratio_h)).save(path.join(clippings_dir, f'{line_id}.png'))
                        clipped += 1
                    
                    else:
                        for string_id, string_dict in line_dict.items():
                            if type(line_dict) is float:
                                continue
                            jp2.crop(ChronAmJP2Clipper.get_box(string_dict, ratio_w, ratio_h)).save(path.join(clippings_dir, f'{string_id}.png'))
                            clipped += 1
        
        print(f'INFO: saved {clipped} clippings to {clippings_dir}.')
        return clipped, clippings_dir

