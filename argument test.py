import argparse


def main2(root_bucket, input_data):
    print(root_bucket)
    print(input_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-rb', '--root_bucket', required=True)  # can 'store_false' for no-xxx flags
    parser.add_argument('-id', '--input_data', required=True)
    parsed = parser.parse_args()
    # NOTE: args with '-' have it replaced with '_'
    main2(parsed.root_bucket, parsed.input_data)