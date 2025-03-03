from os import walk, path
from json import dump
from xml.etree import ElementTree as ET

class ChronAmXMLProcessor:
    """Processes Chronicling America XML files into JSON files containing text and bounding box information.
    
    Attributes:
        files (list[str]) : a list of filepaths
    """

    def __init__(self, data_dir: str) -> None:
        """Initializes a `ChronAmXMLProcessor` from a data directory by scanning for XML files."""
        self.files = []
        for root, _, files in walk(data_dir):
            for file in files:
                if file.endswith('xml'):
                    self.files.append(path.join(root, file))

        print(f'INFO: found {len(self.files)} XML files in directory {data_dir}')
    
    @staticmethod
    def process_xml(filepath: str, include_bounding_box=False, overwrite=False) -> str:
        """Processes the XML file at `filepath` into a JSON file in the same directory.
        
        Arguments:
            filepath             (str)  : the file to process; output will be written to a JSON file in the same directory.
            include_bounding_box (bool) : whether to include bounding box data in JSON output; default False.
            overwrite            (bool) : whether to overwrite existing XML; default False.
        
        Returns:
            _ (str) : the path to the JSON file.

        """

        def add_bounding_box(root: ET.Element, dic: dict) -> None:
            """Utility function for reading bounding box data from `root` into `dic`."""
            left,  upper = int(root.attrib['HPOS']), int(root.attrib['VPOS'])
            right, lower = left + int(root.attrib["WIDTH"]), upper + int(root.attrib['HEIGHT'])
            dic['left'], dic['upper'], dic['right'], dic['lower'] = left, upper, right, lower

        if not filepath.endswith('xml'):
            print(f'WARNING: file {filepath} must be an XML file.')
        
        root = ET.parse(filepath).getroot() or ET.Element('')
        schema = root[0].tag.split('}')[0] + '}'

        for subtag in ('Layout', 'Page', 'PrintSpace'):
            root = root.find(f'{schema}{subtag}') or ET.Element('')
        
        if root.tag == '':
            raise ValueError(f'Failed to parse XML file at {filepath}')
        
        page_dict = {}
        for block in root.findall(f'{schema}TextBlock'):
            block_dict = {}
            if include_bounding_box: add_bounding_box(block, block_dict)
            for line in block.findall(f'{schema}TextLine'):
                line_dict = {}
                if include_bounding_box: add_bounding_box(line, line_dict)
                for string in line.findall(f'{schema}String'):
                    string_dict = {'content': string.attrib['CONTENT']}
                    if include_bounding_box: add_bounding_box(string, string_dict)
                    line_dict[string.attrib['ID']] = string_dict
                if string.attrib.get('SUBS_TYPE', '') == 'HypPart1':
                    line_dict['HYPHEN'] = True
                block_dict[line.attrib['ID']] = line_dict
            page_dict[block.attrib['ID']] = block_dict
        
        json_path = path.join(path.dirname(filepath), path.basename(filepath).replace('xml', 'json'))
        with open(json_path, 'w') as fp:
            dump(page_dict, fp, indent=4)

        return json_path
    
    def process_all(self, include_bounding_box=False, overwrite=False) -> list[str]:
        """Processes all of the XML files in `self.files` into JSON files using `process_xml`; returns a list of the files written."""
        return [ChronAmXMLProcessor.process_xml(filepath, include_bounding_box, overwrite) for filepath in self.files]
