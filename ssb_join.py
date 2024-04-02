import csv


def simple_sort_based_join(product_filename, maker_filename, output_filename):
    """
    Executes a simple sort-based join between two datasets and writes the output to a CSV file.

    :param product_filename: Filename of the sorted product dataset.
    :param maker_filename: Filename of the sorted maker dataset.
    :param output_filename: Filename for the output joined dataset.
    """
    with open(product_filename, 'r') as product_file, open(maker_filename, 'r') as maker_file, open(output_filename, 'w', newline='') as out:
        product_csv = csv.reader(product_file)
        maker_csv = csv.reader(maker_file)
        output_file = csv.writer(out)
        product_page = next(product_csv, None)
        maker_page = next(maker_csv, None)
        joinNum = 0
        while product_page is not None and maker_page is not None:
            product_attr = product_page[0]
            maker_attr = maker_page[0]
            if product_attr == maker_attr:
                product_page.append(maker_page[1])
                output_file.writerow(product_page)
                product_page = next(product_csv, None)
                maker_page = next(maker_csv, None)
                joinNum += 1
            elif product_attr > maker_attr:
                maker_page = next(maker_csv, None)
            else: # product_attr < maker_attr
                product_page = next(product_csv, None)

        print(f"Join {joinNum} rows successfully, please find output in {output_filename}")


# simple_sort_based_join("product_sorted.csv", "maker_sorted.csv", "joined_data.csv")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python ssb_join.py <product_file.csv> <maker_file.csv> <output_file.csv>")
    else:
        simple_sort_based_join(sys.argv[1], sys.argv[2], sys.argv[3])
