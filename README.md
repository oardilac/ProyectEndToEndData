<a name="readme-top"></a>
[![Contributors](https://shields.io/badge/Contributors-1-green)](https://github.com/oardilac/ProyectEndToEndData/graphs/contributors)
[![Forks](https://img.shields.io/github/forks/oardilac/Conversational-AI-Chatbot)](https://github.com/oardilac/ProyectEndToEndData/network/members)
[![Stargazers](https://img.shields.io/github/stars/oardilac/Conversational-AI-Chatbot)](https://github.com/oardilac/ProyectEndToEndData/stargazers)
[![Issues](https://img.shields.io/github/issues/oardilac/Conversational-AI-Chatbot)](https://github.com/oardilac/ProyectEndToEndData/issues)
[![MIT License](https://img.shields.io/github/license/oardilac/Conversational-AI-Chatbot)](https://github.com/oardilac/ProyectEndToEndData/blob/main/LICENSE)
[![LinkedIn](https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555)](https://www.linkedin.com/in/oardilac/)
<br />
<div align="center">
    <h3 align="center">Airflow-based ETL Project</h3>

   <p align="center">
    This is a complete project for a retail data processing pipeline. It includes scripts for data extraction, transformation, and loading. The project also comes with a Jupyter notebook for testing the complete flow and visualizing the data. The scripts utilize various technologies such as MySQL, MongoDB, Google Cloud Storage, and Apache Airflow for orchestrating the tasks.
    <br />

  <p align="center">
    <a href="https://github.com/oardilac/ProyectEndToEndData/"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/oardilac/ProyectEndToEndData/">View Demo</a>
    ·
    <a href="https://github.com/oardilac/ProyectEndToEndData/issues">Report Bug</a>
    ·
    <a href="https://github.com/oardilac/ProyectEndToEndData/issues">Request Feature</a>
  </p>
</div>


<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

This project is about creating a data pipeline using Apache Airflow to extract, transform and load data from various data sources into a centralized data warehouse.

The project follows DRY principles to make the development process more efficient, while also making sure that the implementation is understandable and maintainable.

### Features
* Utilizes Docker Compose for environment isolation.
* Uses Apache Airflow for scheduling and monitoring workflows.
* Enables data extraction from MySQL and MongoDB databases, and Google Cloud Storage.
* Allows data transformation using Python and Pandas.
* Supports data loading to Google Cloud Storage and MySQL.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

This section lists major libraries and tools used in the project.

* [Docker](https://www.docker.com/)
* [Python](https://www.python.org/)
* [Airflow](https://airflow.apache.org/)


<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- GETTING STARTED -->
## Getting Started

Follow these steps to get a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites
Make sure you have the following tools installed on your machine before you start:

* Docker
* Python

### Installation

1. Clone the repo

    ```
    git clone https://github.com/oardilac/ProyectEndToEndData.git
    ```

2. Navigate to the project directory:

    ```
    cd ProyectEndToEndData
    ```

3. Run Docker Compose to start up the services:
   ```
   docker-compose up
   ```

4. Access to the jupyter lab of the project with the Python files
    ```
    http://localhost:8200/lab/tree/
    ```

### Airflow Setup
1. Create a Python virtual environment:
    ```
    python -m venv airflow
    ```

2. Activate the virtual environment:
    ```
    source airflow/bin/activate
    ```

3. Set the Airflow home directory:
    ```
    export AIRFLOW_HOME=~/airflow
    ```

4. Install Apache Airflow:
    ```
    pip install apache-airflow
    ```

5. Initialize the Airflow database:
    ```
    airflow db init
    ```

6. Create a new Airflow user:
    ```
    airflow users create -e admin@example.org -f John -l Doe -p admin -r Admin -u admin
    ```

7. Start the Airflow webserver:
    ```
    airflow webserver -p 8080
    ```

8. In a new terminal, start the Airflow scheduler:
    ```
    airflow scheduler
    ```

You can now access the Airflow web interface at http://localhost:8210/.

<!-- Usage -->
## Usage
Once everything is up and running, you can start the ETL process by triggering the DAG in the Airflow web interface.

This will execute the data extraction, transformation, and loading process as defined in the Python scripts.

<!-- ROADMAP -->
## Roadmap

- [x] Add Changelog
- [x] Add back to top links
- [ ] Add Additional Templates w/ Examples
- [ ] Add "components" document to easily copy & paste sections of the readme

See the [open issues](https://github.com/oardilac/ChatGPT-3.5-Django-React/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->
## License
Distributed under the MIT License. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- CONTACT -->
## Contact

Oliver Ardila - odardilacueto@gmail.com - @oardilac

Project Link: [https://github.com/oardilac/ProyectEndToEndData](https://github.com/oardilac/ProyectEndToEndData)

<p align="right">(<a href="#readme-top">back to top</a>)</p>