import json
import pandas as pd
from selenium import webdriver
from model import Job
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time
import boto3

# run local
# event = {
#     'chromepath' : 'E:\DE\Spark\chromedriver-win64\chromedriver.exe',
#     "bucket_name": "tobie2314",
#     "object_key": "raw_zone/job_it_topcv.json"
# }


def lambda_handler(event, context):
    # TODO implement
    try:
        job_list_detail=[]
        url='https://www.topcv.vn/tim-viec-lam-it-phan-mem-tai-ha-noi-l1c10026?page=1'
        chromepath = event['chromepath']
        driver = webdriver.Chrome(executable_path=chromepath)
        driver.get(url)
        for next in range(1,54):
            job_items=driver.find_elements(By.CLASS_NAME,'job-item-search-result')
            quick_view_button=job_items[0].find_elements(By.XPATH,"//div[@class='body']/div[@class='body-content']/div[@class='title-block']/div[@class='box-right']/p[@class='quick-view-job-detail']")
            for job_detail_view in quick_view_button:
                    # ấn nút xem nhanh chi tiết
                    job_detail_view.click()
                    time.sleep(1)
                    
                    header_job= driver.find_elements(By.XPATH,"//div[@class='box-view-job-detail']/div[@class='box-header']")
                    body_job= driver.find_elements(By.XPATH,"//div[@class='box-view-job-detail']/div[@class='box-scroll']")
                    if(len(header_job)>0 and len(body_job)>0):
                            header_job=header_job[0]
                            body_job=body_job[0]
                            job= Job()
                            job.title=header_job.find_elements(By.XPATH,"//div[@class='box-title']/h2[@class='title']")[0].text
                            job.salary=header_job.find_elements(By.XPATH,"//div[@class='header-normal-default']/div[@class='box-info-job']/div[@class='box-info-header']/div[@class='box-item-header'][1]/div[@class='box-item-value']")[0].text
                            job.location=driver.find_elements(By.XPATH,"//div[@class='header-normal-default']/div[@class='box-info-job']/div[@class='box-info-header']/div[@class='box-item-header'][2]/div[@class='box-item-value']")[0].text
                            job.exp=header_job.find_elements(By.XPATH,"//div[@class='header-normal-default']/div[@class='box-info-job']/div[@class='box-info-header']/div[@class='box-item-header'][3]/div[@class='box-item-value']")[0].text
                            job.description=body_job.find_elements(By.XPATH,"//div[@class='box-job-info']/div[@class='content-tab'][1]")[0].get_attribute("innerHTML")
                            job.requirement=body_job.find_elements(By.XPATH,"//div[@class='box-job-info']/div[@class='content-tab'][2]")[0].get_attribute("innerHTML")
                            job.benefit=body_job.find_elements(By.XPATH,"//div[@class='box-job-info']/div[@class='content-tab'][3]")[0].get_attribute("innerHTML")
                            job.working_location=body_job.find_elements(By.XPATH,"//div[@class='box-job-info']/div[@class='box-address']/div/div")[0].text
                            if(len(body_job.find_elements(By.XPATH,"//div[@class='box-job-info']/div[@class='content-tab'][4]/div[@class='content-tab__list']"))>0):
                                    job.working_time=body_job.find_elements(By.XPATH,"//div[@class='box-job-info']/div[@class='content-tab'][4]/div[@class='content-tab__list']")[0].text
                            job.company=body_job.find_elements(By.XPATH,"//div[@class='job-detail__company']/div[@class='job-detail__company--information']/div[@class='job-detail__company--information-item company-name']/div[@class='box-main']/h2[@class='company-name-label']/a[@class='name']")[0].text
                            job_list_detail.append(job)
            driver.find_elements(By.CLASS_NAME,"pagination")[0].find_elements(By.TAG_NAME,"li")[2].click()
            time.sleep(4)
    except Exception as ex:
        print(f'Error: {ex}')
        
    #load to s3
    bucket_name = event["bucket_name"]
    object_key = event["object_key"]

    df = pd.DataFrame(job_list_detail)

    load_to_s3(df,bucket_name,object_key)

def load_to_s3(df,bucket_name,object_key):
    df_results = []

    for index, row in df.iterrows():
        df_result = row.to_dict()
        df_results.append(df_result)

    #save result to s3
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket_name,object_key)

    s3_object.put(
        Body=(bytes(json.dumps(df_results, default=str).encode('UTF-8')))
    )