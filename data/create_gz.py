import gzip

input_path = "data/sample_abr.xml"
output_path = "data/sample_abr.xml.gz"

with open(input_path, "rb") as f_in:
    with gzip.open(output_path, "wb") as f_out:
        f_out.writelines(f_in)

print("GZ file created successfully:", output_path)
