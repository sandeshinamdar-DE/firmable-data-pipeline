import json
import sys
from bs4 import BeautifulSoup

def extract_from_html(html_text):
    soup = BeautifulSoup(html_text, "html.parser")

    title = None
    if soup.title:
        title = soup.title.string.strip()

    industry = None
    m = soup.find("meta", attrs={"name": "industry"})
    if m and m.get("content"):
        industry = m["content"].strip()

    return title, industry


def parse_wet_file(wet_path, out_path):
    with open(wet_path, "r", encoding="utf-8") as f, open(out_path, "w", encoding="utf-8") as out:

        url = None
        html_buffer = []
        inside_html = False

        for line in f:
            line = line.rstrip("\n")

            # Detect URL
            if line.startswith("WARC-Target-URI:"):
                # flush previous block
                if url and html_buffer:
                    html = "\n".join(html_buffer)
                    name, industry = extract_from_html(html)
                    record = {
                        "website_url": url,
                        "company_name": name,
                        "industry": industry
                    }
                    out.write(json.dumps(record) + "\n")

                url = line.split(" ", 1)[1].strip()
                html_buffer = []
                inside_html = False
                continue

            # Detect start of HTML
            if line.startswith("<html"):
                inside_html = True

            # Capture HTML
            if inside_html:
                html_buffer.append(line)

            # Detect end of HTML
            if line.startswith("</html>"):
                inside_html = False

        # final flush
        if url and html_buffer:
            html = "\n".join(html_buffer)
            name, industry = extract_from_html(html)
            record = {
                "website_url": url,
                "company_name": name,
                "industry": industry
            }
            out.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    parse_wet_file(sys.argv[1], sys.argv[2])
