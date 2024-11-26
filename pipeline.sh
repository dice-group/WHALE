# This pipeline will do the following:

# (1) Get the datasets (individual paths/directory path) from the user
# (2) Give this path to dice-embeddings to create embeddings

# - From here the paths diverge

# Path A: (3) Use evaluation mode directly on the dataset which has `dataset_1` in their name to get the evaluation results from query generator
# Path B: (3) Create class and property alignment from AML tool.
#         (4) Create config files for the dataset for Limes.
#         (5) Create sameAs links from Limes.
#         (6) Give the embeddings results and Limes result to Procrustes algorithm.
#         (7) Provide the input to query generator.
