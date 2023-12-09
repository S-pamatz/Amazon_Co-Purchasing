
def parseBestSellingCategories(file_path):
    authors = []
    categories = []
    aCount = 1
    cCount = 1

    with open(file_path, 'r') as file:
        next(file)  # Skip the first line
        for line in file:
            line = line.strip()
            if line.startswith('"'):
                if aCount <= 100:
                    line = line.replace('"', '')  # remove quotation marks
                    name = line.split(',')[0]  # extract the name
                    authors.append((aCount, name))
                    aCount += 1
            else:
                if cCount <= 100:
                    name = line.split(',')[0]  # extract the category name
                    categories.append((cCount, name))
                    cCount += 1

    return categories, authors

