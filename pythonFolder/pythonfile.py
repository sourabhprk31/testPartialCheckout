import pandas as pd
def tableFromText(text, excelname):
    text = text.split("###")
    rows = text[2].strip().split('\n')
    headers = [header.strip() for header in rows[0].split('|') if header.strip()]
    data = [[cell.strip() for cell in row.split('|') if cell.strip()] for row in rows[2:]]
    data = [[cell.strip() for cell in row] for row in data]
    df = pd.DataFrame(data, columns=headers)
    df.to_excel(excelname, index=False)
def TableToText(table):
    if not all(len(row) == len(table[0]) for row in table):
        raise ValueError("All inner lists must have the same length")
    table_string = "| " + " | ".join(table[0]) + " |\n"
    table_string += "| " + "| ".join(["-" * len(cell) for cell in table[0]]) + " |\n"
    for row in table[1:]:
        table_string += "| " + " | ".join(row) + " |\n"
    return table_string