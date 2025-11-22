import json
import gzip
from lxml import etree
import sys
from datetime import date

def validate_abn(abn_str):
    try:
        weights = [10,1,3,5,7,9,11,13,15,17,19]
        digits = [int(d) for d in abn_str]
        digits[0] -= 1
        total = sum(w * d for w, d in zip(weights, digits))
        return total % 89 == 0
    except:
        return False


def parse_abr_xml_gz(gz_path, output_path):
    """
    Extract ABR XML into JSONL format
    """
    with gzip.open(gz_path, 'rb') as f:
        tree = etree.parse(f)
        root = tree.getroot()

        with open(output_path, 'w', encoding='utf-8') as out:
            for entity in root.findall(".//Entity"):
                rec = {
                    "abn": entity.findtext("ABN"),
                    "entity_name": entity.findtext("EntityName"),
                    "entity_type": entity.findtext("EntityType"),
                    "entity_status": entity.findtext("EntityStatus"),
                    "address": entity.findtext("AddressLine"),
                    "postcode": entity.findtext("Postcode"),
                    "state": entity.findtext("State"),
                    "start_date": entity.findtext("StartDate"),
                    "abn_valid": validate_abn(entity.findtext("ABN")),
                    "ingest_date": str(date.today())
                }
                out.write(json.dumps(rec) + "\n")

if __name__ == "__main__":
    # Usage: python extract_abr.py input.xml.gz output.jsonl
    gz_path = sys.argv[1]
    output = sys.argv[2]
    parse_abr_xml_gz(gz_path, output)
