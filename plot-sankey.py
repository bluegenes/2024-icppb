import pandas as pd
import plotly.graph_objects as go

# Load the main data file
data = pd.read_csv('barcode16.k31.gather.with-lineages.csv')
df = data[["f_unique_weighted", "lineage"]]

# Load the lineage mapping file
lineage_mapping = pd.read_csv('inputs/LINgroups.csv')

# Function to find the longest prefix match
def find_longest_prefix(lineage, mapping):
    if lineage == '':
        return "unclassified"
    lineage_parts = lineage.split(';')
    for i in range(len(lineage_parts), 0, -1):
        prefix = ';'.join(lineage_parts[:i])
        match = mapping[mapping['lin'] == prefix]
        if not match.empty:
            return match.iloc[0]['name']
    # return "A_Total_reads;B_Tomato"
    return "unclassified"

df['lineage'] = df['lineage'].fillna('Tomato') # avoid issues with NaN (unclassified)
df['name'] = df['lineage'].apply(find_longest_prefix, mapping=lineage_mapping)
df['name'] = df['name'].fillna('A_Total_reads;B_Tomato')

# Split the 'name' column into components
df['name_split'] = df['name'].str.split(';')

# Function to determine the level based on prefix
def determine_level(name):
    last_segment = name.split(';')[-1]
    if last_segment.startswith('A_'):
        return 0
    elif last_segment.startswith('B_'):
        return 1
    elif last_segment.startswith('C_'):
        return 2
    elif last_segment.startswith('D_'):
        return 3
    elif last_segment.startswith('E_'):
        return 4
    else:
        return 5  # Default level for unclassified or other levels

# Create lists to store the nodes and links
nodes = []
links = []
node_levels = {}
from collections import defaultdict
total_weights = defaultdict(int)

# Create a set to store unique nodes
unique_nodes = set()

# Loop through the data to populate nodes and links
for i, row in df.iterrows():
    lineage_names = row['name_split']
    weight = row['f_unique_weighted']
    print("lineage_names:", lineage_names)
    # print("weight:", weight)
    # Loop through the lineage names to create nodes and links
    for j in range(len(lineage_names) - 1):
        source = lineage_names[j]
        print("source:", source)
        target = lineage_names[j + 1]
        print("target:", target)
        # Add source and target to the set of unique nodes
        unique_nodes.add(source)
        unique_nodes.add(target)
        # Add link to the list of links
        links.append({'source': source, 'target': target, 'value': weight})
        # Track the level of each node
        node_levels[source] = determine_level(source)
        node_levels[target] = determine_level(target)
        # total_weights[source] + weight
        # total_weights[target] + weight

# Ensure all nodes are accounted for in node_levels
for node in unique_nodes:
    if node not in node_levels:
        node_levels[node] = determine_level(node)

# Convert the set of unique nodes to a list and create a dictionary to map node names to indices
unique_nodes = list(unique_nodes)
node_indices = {node: i for i, node in enumerate(unique_nodes)}

# # Debug print statements to verify data processing
print("Unique Nodes:", unique_nodes)
print("Node Levels:", node_levels)
print("Node Indices:", node_indices)

# Group nodes by levels
nodes_by_level = {}
for node, level in node_levels.items():
    if level not in nodes_by_level:
        nodes_by_level[level] = []
    nodes_by_level[level].append(node)

# Determine the maximum level for custom positioning
max_level = max(node_levels.values())

# Create x and y positions for nodes based on their levels
x_positions = [node_levels[node] / max_level for node in unique_nodes]
y_positions = []

# Adjust y positions to ensure proper spacing
for level in range(max_level + 1):
    if level in nodes_by_level:
        nodes_at_level = nodes_by_level[level]
        total_nodes_at_level = len(nodes_at_level)
        spacing = 1 / (total_nodes_at_level + 1)
        for i, node in enumerate(nodes_at_level):
            y_positions.append((i + 1) * spacing)

# Debug print statements to verify positions
print("X Positions:", x_positions)
print("Y Positions:", y_positions)

# Convert links to the format required by Plotly
plotly_links = {
    'source': [node_indices[link['source']] for link in links],
    'target': [node_indices[link['target']] for link in links],
    'value': [link['value'] for link in links],
}

# Debug print statements to verify links
print("Plotly Links:", plotly_links)

# Create the Sankey plot
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=20,  # Increase the padding between nodes
        thickness=30,  # Increase the thickness of the nodes
        line=dict(color="black", width=0.5),
        label=unique_nodes,
        # x=x_positions,
        # y=y_positions
    ),
    link=plotly_links,
    arrangement="freeform",
)])

fig.update_layout(title_text="barcode 16", font_size=10)
fig.show()
fig.write_image("bc16.sankey.png")
