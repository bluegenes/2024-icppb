import pandas as pd

# Read the lin taxonomy and lingroups
file1 = pd.read_csv('LINgroups.csv')
file2 = pd.read_csv('ralstonia32.lin-taxonomy.csv')

# Function to determine the category based on lin column
def determine_category(lin, file1):
    for index, row in file1.iterrows():
        if row['lin'] in lin:
            category = row['name']
            if category.startswith("A_Total_reads;B_"):
                category = category.replace("A_Total_reads;B_", "", 1)
            return category
    return None

# Create the third file DataFrame
categories = pd.DataFrame(columns=['label', 'category'])

# List to hold the rows for the new DataFrame
rows = []

# Iterate through file2 and populate the third file
for index, row in file2.iterrows():
    label = f"{row['accession']} {row['species']} {row['strain']}"
    category = determine_category(row['lin'], file1)
    rows.append({'label': label, 'category': category})

# Convert the list of rows to a DataFrame
categories = pd.DataFrame(rows, columns=['label', 'category'])

# Save the third file to CSV
categories.to_csv('ralstonia32.phylotypes.csv', index=False)

print("Category file generated successfully.")

