from argparse import ArgumentParser
from es_utils import get_ids


if __name__ == "__main__":
    ap = ArgumentParser(description="dump unique game IDs from users in ES to a file")
    ap.add_argument("-i", "--index", default="rita", help="Elasticsearch Index")
    ap.add_argument("-d", "--doctype", default="user", help="Document type")
    ap.add_argument("-f", "--field", default="games_id_list", help="Nested field name (value must be a list)")
    ap.add_argument("-o", "--output", default="games_nested.txt", help="Output filepath")
    args = ap.parse_args()
    with open(args.output, "w") as fh:
        for a in get_ids(args.index, args.doctype, nested_field=args.field):
            fh.write(str(a)+"\n")
