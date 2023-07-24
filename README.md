# Stock data pipeline

In this project, I collect all tickers in the S&P 500, and then I created a pipeline that runs hourly using Python to push the related data gotten from yfinance to Google BigQuery for storing and querying, with Apache Airflow is used for workflow orchestration, Docker is used to run Airflow locally, and everything is running on a Google Cloud VM instance. From here, I will create a dashboard including intraday data and text analyzed data in order to understand how the stock market is running. 

Prior to this project, I have created a similar project before with some AWS services, but now I would want to make a more "formal" and "better" pipeline with more conventional tools. Special thanks to [DTC Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) for your repo and your course has inspired me to "restarted" this project with better tools and better practices. 

Feel free to look at my project and give me a star if you think it is good! Thank you^^

## 🔧 Built With

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

## :triangular_flag_on_post: Roadmap

- [x] Create GCP account + configure VM instance
- [x] Set up Python, Docker, Airflow GCP config on VM 
- [x] Create pipeline for uploading intraday stock data of 500 stocks hourly to GCP
- [x] Create pipeline for uploading text data to GCP
- [x] Create pipeline for uploading financial statement, financial ratio data to GCP
- [ ] Create dashboard
- [x] Improve pipeline to support more data uploading (minute instead of hourly data)

## :closed_book: Acknowledgments

Use this space to list resources you find helpful and would like to give credit to. I've included a few of my favorites to kick things off!

* [GitHub Emoji Cheat Sheet](https://www.webpagefx.com/tools/emoji-cheat-sheet)
* [Github Bagde](https://github.com/Ileriayo/markdown-badges)
* [DTC Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
