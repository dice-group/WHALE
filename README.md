
# WHALE: Web-Scale Hybrid Explainable Machine Learning

WHALE (Web-Scale Hybrid Explainable Machine Learning) is a pioneering project that aims to develop time-efficient, explainable machine learning models that can operate on web-scale RDF knowledge graphs. By leveraging the power of large-scale language models and innovative hybrid class expression learning (CEL) approaches, WHALE seeks to create scalable, reusable models that enhance the transparency and trustworthiness of AI decisions, particularly in complex web environments.

## Overview

The Web has become the largest and most widely used information infrastructure globally, hosting vast amounts of data in the form of RDF knowledge graphs. These knowledge graphs play a critical role in various applications, from scientific research to everyday use on platforms like Google and Facebook.

### Key Goals

- **Explainability**: Enhance trust in AI systems by making their decisions explainable, especially when working with massive web-scale knowledge graphs.
- **Efficiency**: Develop time-efficient methods for CEL on expressive description logics (DLs) like SROIQ(D), which are commonly used in RDF knowledge bases.
- **Scalability**: Create models that can efficiently process and link extremely large datasets, ensuring they are applicable to a broad range of web-scale data applications.

## Features

- **Hybrid Class Expression Learning (CEL)**: Combines multiple representations of knowledge to accelerate the learning process, making it feasible to apply CEL on large-scale knowledge graphs.
- **Universal Knowledge Graph Embeddings**: Development of embeddings for large-scale knowledge graphs, enabling the training of deep learning models that act as efficient function approximators during the CEL process.
- **Tensor-Based Querying**: Use of tensor representations to improve the runtime of queries on RDF data, facilitating instance retrieval on a web scale.

## Installation

To install and run WHALE, follow these steps:

1. **Clone the repository**:
    ```bash
    git clone https://github.com/dice-group/WHALE.git
    cd WHALE
    ```

2. **Set up a virtual environment (optional but recommended)**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

<!-- TODO: ADD ## Usage -->


<!-- For detailed examples and API documentation, please refer to the official [WHALE documentation](https://github.com/dice-group/WHALE/wiki). -->

## Research and Development

WHALE is a result of collaborative efforts involving experts in multi-processing deep learning techniques, knowledge graph embeddings, and tensor representations. The project involves a multi-step workflow:

1. **Data Gathering**: Collection of large-scale RDF knowledge graphs.
2. **Preprocessing**: Transformation of RDF data for compatibility with various tools and libraries.
3. **Training**: Development of knowledge graph embeddings using hybrid CEL approaches.
4. **Linking**: Integration of different knowledge graphs to create a unified dataset.
5. **Model Training**: Training of large language models (LLMs) on the unified embeddings.
6. **Benchmarking**: Evaluation of the trained models to ensure performance and accuracy.

<!-- TODO: ADD 

## Contributing

Contributions to WHALE are welcome! To contribute:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Make your changes and ensure that your code adheres to our coding standards.
4. Submit a pull request detailing your changes.

## License

WHALE is released under the [MIT License](LICENSE). -->

## Acknowledgements

WHALE is supported by the Lamarr Fellowship and developed at Paderborn University by Prof. Dr. Axel Ngonga and his team. The project also collaborates with the Lamarr Network and various other academic and research institutions.

For any inquiries or support, please contact the maintainers at [sshivam@mail.uni-paderborn.de](mailto:sshivam@mail.uni-paderborn.de).
