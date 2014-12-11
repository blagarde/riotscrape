from argparse import ArgumentParser
from es_utils import get_ids


if __name__ == "__main__":
    ap = ArgumentParser(description="dump game IDs to a file")
    ap.add_argument("-i", "--index", default="rito", help="Elasticsearch Index")
    ap.add_argument("-d", "--doctype", default="game", help="Document type")
    ap.add_argument("-o", "--output", default="games.txt", help="Output filepath")
    args = ap.parse_args()
    with open(args.output, "w") as fh:
        for a in get_ids(args.index, args.doctype):
            fh.write(str(a)+"\n")
