"""
Copy every file from another user's S3 "diffusion" folder into your own one,
on the Onyxia (SSP Cloud) MinIO storage.

Files already in your folder with the same size are skipped, so the script can
be run again to fetch only new files. A file with a different size (an older
version or an interrupted copy) is copied again.

S3 credentials and endpoint are read from the AWS_* environment variables that
Onyxia injects into the service, and your bucket name from VAULT_TOP_DIR (your
Onyxia username), so no configuration is needed. Usage:
    python3 S3_to_S3_tools/sync_diffusion.py              # copy from bastienlevyguinot/diffusion
    python3 S3_to_S3_tools/sync_diffusion.py --dry-run    # only list what would be copied
    python3 S3_to_S3_tools/sync_diffusion.py --source otheruser/diffusion --dest me/diffusion
"""
import argparse
import os

import s3fs

DEFAULT_SOURCE = "bastienlevyguinot/diffusion"


def copy(fs, src, dst):
    """Server-side copy, falling back to streaming through this machine if it is refused."""
    try:
        fs.copy(src, dst)
    except (PermissionError, OSError):
        with fs.open(src, "rb") as fin, fs.open(dst, "wb") as fout:
            while chunk := fin.read(64 * 2**20):
                fout.write(chunk)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default=DEFAULT_SOURCE, help=f"folder to copy from (default: {DEFAULT_SOURCE})")
    parser.add_argument("--dest", default=None, help="folder to copy to (default: <your username>/diffusion)")
    parser.add_argument("--dry-run", action="store_true", help="list the files to copy without copying them")
    args = parser.parse_args()

    source = args.source.removeprefix("s3://").rstrip("/")
    dest = (args.dest or f"{os.environ['VAULT_TOP_DIR']}/diffusion").removeprefix("s3://").rstrip("/")
    if source == dest:
        raise SystemExit(f"Source and destination are the same folder: {source}")

    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    src_files = fs.find(source, detail=True)
    dst_sizes = {p.removeprefix(dest + "/"): info["size"]
                 for p, info in fs.find(dest, detail=True).items()}
    if not src_files:
        raise SystemExit(f"No files found in s3://{source}")

    n_copied = n_skipped = 0
    for src, info in sorted(src_files.items()):
        rel = src.removeprefix(source + "/")
        size_mb = info["size"] / 2**20
        if dst_sizes.get(rel) == info["size"]:
            print(f"skip  {rel} ({size_mb:,.1f} MB, already there)")
            n_skipped += 1
            continue
        print(f"copy  {rel} ({size_mb:,.1f} MB)", flush=True)
        if not args.dry_run:
            copy(fs, src, f"{dest}/{rel}")
        n_copied += 1

    verb = "to copy" if args.dry_run else "copied"
    print(f"\n{n_copied} file(s) {verb}, {n_skipped} skipped: s3://{source} -> s3://{dest}")


if __name__ == "__main__":
    main()
