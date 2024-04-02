import csv
import os

CHUNK_SIZE = 2
MERGE_SIZE = 2


def sort_and_write_chunk(chunk, run_id):
    """
    Sorts a chunk of data in-memory by the first column and writes it to a temporary file.
    
    :param chunk: A list of data rows to be sorted.
    :param run_id: Identifier for the run, used for naming the temporary file.
    """
    filename = f"temp_chunk_{run_id}.csv"
    sorted_chunk = sorted(chunk, key=lambda l: l[0])
    # print("sorted chunk #", run_id)
    with open(filename, 'w', newline='') as f:
        temp_file = csv.writer(f)
        temp_file.writerows(sorted_chunk)
    return filename


def merge_runs(run_files, output_filename):
    """
    Merges sorted files (runs) into a single sorted output file.
    
    :param run_files: List of filenames representing sorted runs to be merged.
    :param output_filename: Filename for the merged, sorted output.
    """
    print(f"Merging: {run_files[0]} and {run_files[1]}")
    with open(run_files[0], 'r') as f1, open(run_files[1], 'r') as f2, open(output_filename, 'w', newline='') as out:
        csv1 = csv.reader(f1)
        csv2 = csv.reader(f2)
        output_file = csv.writer(out)
        page1 = next(csv1, None)
        page2 = next(csv2, None)
        while page1 is not None and page2 is not None:
            if page1[0] < page2[0]:
                # print(page1[0] + " < " + page2[0])
                output_file.writerow(page1)
                page1 = next(csv1, None)
            else:
                # print(page1[0] + " < " + page2[0])
                output_file.writerow(page2)
                page2 = next(csv2, None)
        while page1 is not None:
            # print(page1[0])
            output_file.writerow(page1)
            page1 = next(csv1, None)
        while page2 is not None:
            # print(page2[0])
            output_file.writerow(page2)
            page2 = next(csv2, None)
    return output_filename


def external_sort(input_filename, output_filename):
    """
    the external sort process: chunking, sorting, and merging.
    
    :param input_filename: Name of the file with data to sort.
    :param output_filename: Name of the file where sorted data will be written.
    """
    run_id = 0
    temp_filenames = []
    with open(input_filename, 'r', newline='') as f:
        csv_file = csv.reader(f)
        chunk = []
        print(f"{'=' * 5}Sorting{'=' * 5}")
        for i, row in enumerate(csv_file):
            chunk.append(row)
            if i % CHUNK_SIZE == 1 and i > 0:
                temp_filename = sort_and_write_chunk(chunk, run_id)
                temp_filenames.append(temp_filename)
                run_id += 1
                chunk.clear()

    if chunk:
        temp_path = sort_and_write_chunk(chunk, run_id)
        temp_filenames.append(temp_path)
        run_id += 1

    print(f"Sorted {run_id+1} chunks successfully!!")

    # MERGING
    print(f"{'=' * 5}Merging{'=' * 5}")
    passNum = 1
    while len(temp_filenames) > 1:
        print("Merging pass #", passNum)
        new_temp = []
        merge_page = []
        for i, temp_filename in enumerate(temp_filenames):
            # print(i, temp_filename)
            merge_page.append(temp_filename)
            if i % MERGE_SIZE == 1:
                base_name, extension = os.path.splitext(output_filename)
                merge_filename = base_name + f"_{passNum}_{int(i/2)}" + extension
                new_temp.append(merge_runs(merge_page, merge_filename))
                delete_temp_files(merge_page)
                merge_page.clear()
        if merge_page:
            odd_file = merge_page.pop()
            print("Keep odd chunk to the next pass: "+ odd_file)
            delete_temp_files(merge_page)
            new_temp.append(odd_file)
            print(merge_page)
        temp_filenames = new_temp
        # print(temp_filenames)
        passNum += 1

    os.rename(temp_filenames[0], output_filename)
    print(f"\nExternal sorted successfully within {passNum-1} pass(es)!! File saved as {output_filename}")


def delete_temp_files(temp_filenames):
    for filename in temp_filenames:
        os.remove(filename)


# external_sort('product.csv', 'product_sorted.csv')

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python3 ext_sort.py input.csv output.csv")
    else:
        input_filename = sys.argv[1]
        output_filename = sys.argv[2]
        external_sort(input_filename, output_filename)
