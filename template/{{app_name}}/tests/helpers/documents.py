import io

from PIL import Image


# Add a blank page (A4 dimensions in points: 1/72 inch per unit)
# 8.27 * 72 = 595.44, 11.7 * 72 = 842.4
def generate_blank_pdf(num_pages=1, width=595, height=842) -> bytes:
    """Generate a blank PDF with specified number of pages."""
    from pypdf import PdfWriter

    # Create a writer object
    writer = PdfWriter()

    for _i in range(num_pages):
        writer.add_blank_page(width=width, height=height)

    pdf_bytes = io.BytesIO()
    writer.write(pdf_bytes)

    return pdf_bytes.getvalue()


def generate_blank_pdf_image(num_pages=1, width=100, height=100) -> bytes:
    """Generate a blank PDF with specified number of pages."""
    images = [Image.new("RGB", (width, height), "white") for _ in range(num_pages)]
    pdf_bytes = io.BytesIO()

    if num_pages == 1:
        images[0].save(pdf_bytes, format="PDF")
    else:
        images[0].save(pdf_bytes, format="PDF", save_all=True, append_images=images[1:])

    return pdf_bytes.getvalue()


def generate_blank_tiff(num_pages=1, width=100, height=100) -> bytes:
    """Generate a blank TIFF with specified number of pages."""
    images = [Image.new("RGB", (width, height), "white") for _ in range(num_pages)]
    tiff_bytes = io.BytesIO()

    if num_pages == 1:
        images[0].save(tiff_bytes, format="TIFF")
    else:
        images[0].save(tiff_bytes, format="TIFF", save_all=True, append_images=images[1:])

    return tiff_bytes.getvalue()


def generate_blank_image(format="JPEG", width=100, height=100) -> bytes:
    """Generate a blank image in specified format."""
    img = Image.new("RGB", (width, height), "white")
    img_bytes = io.BytesIO()
    img.save(img_bytes, format=format)
    return img_bytes.getvalue()
