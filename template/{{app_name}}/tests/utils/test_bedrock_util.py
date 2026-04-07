import json
from pathlib import Path

from documentai_api.utils.bedrock import assess_image_quality

MEDIA_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".webp": "image/webp",
}

doc_dir = Path("/Users/lgoolsby/Development/idp-platform-copa/tests/documents/all")

# start with a few interesting ones
test_files = [
    "test-000-blurry_paystub.jpg",  # blurry
    "test-108-w2-blurry.jpg",  # blurry
    "test-112-w4-blurry.jpeg",  # blurry
    "test-024-w2.jpg",  # clean doc
    "test-020-passport-us.jpg",  # clean doc
    "test-053-il-driverslicense.jpg",  # clean doc
    "test-059-catimage.jpg",  # not a document
]

for name in test_files:
    path = doc_dir / name
    suffix = path.suffix.lower()
    media_type = MEDIA_TYPES.get(suffix)

    if not media_type:
        print(f"SKIP {name} (unsupported type)")
        continue

    result = assess_image_quality(path.read_bytes(), media_type)
    print(f"{name}: {json.dumps(result, indent=2)}")
    print()
