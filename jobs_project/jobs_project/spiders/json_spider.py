import json
import scrapy

from jobs_project.items import JobItem

class JobSpider(scrapy.Spider):
	
	name = 'json_spider'
	custom_settings = {
    	'ITEM_PIPELINES': {
        		'jobs_project.pipelines.PostgreSQLPipeline': 300,
    	},
	}

	def __init__(self, **kwargs):
		self.start_requests()
	
	def parse_page(self, response):
		all_info = json.loads(response.text)
		jobs = all_info['jobs']
		for job in jobs:
			job_data = job['data']
			job_item = JobItem(
				slug=job_data.get("slug"),
				language=job_data.get("language"),
				languages=job_data.get("languages"),
				req_id=job_data.get("req_id"),
				title=job_data.get("title"),
				description=job_data.get("description"),
				street_address=job_data.get("street_address"),
				city=job_data.get("city"),
				state=job_data.get("state"),
				country_code=job_data.get("country_code"),
				postal_code=job_data.get("postal_code"),
				location_type=job_data.get("location_type"),
				latitude=job_data.get("latitude"),
				longitude=job_data.get("longitude"),
				categories=job_data.get("categories"),
				tags=job_data.get("tags"),
				tags5=job_data.get("tags5"),
				tags6=job_data.get("tags6"),
				brand=job_data.get("brand"),
				promotion_value=job_data.get("promotion_value"),
				salary_currency=job_data.get("salary_currency"),
				salary_value=job_data.get("salary_value"),
				salary_min_value=job_data.get("salary_min_value"),
				salary_max_value=job_data.get("salary_max_value"),
				benefits=job_data.get("benefits"),
				employment_type=job_data.get("employment_type"),
				hiring_organization=job_data.get("hiring_organization"),
				source=job_data.get("source"),
				apply_url=job_data.get("apply_url"),
				internal=job_data.get("internal"),
				searchable=job_data.get("searchable"),
				applyable=job_data.get("applyable"),
				li_easy_applyable=job_data.get("li_easy_applyable"),
				ats_code=job_data.get("ats_code"),
				update_date=job_data.get("update_date"),
				create_date=job_data.get("create_date"),
				category=job_data.get("category"),
				full_location=job_data.get("full_location"),
				short_location=job_data.get("short_location")
				)
			yield job_item
	
	def start_requests(self):
		start_urls = ['file:///project_folder/data/s02.json',
				'file:///project_folder/data/s01.json']

		for url in start_urls:
		    yield scrapy.Request(
        	    url=url, 
        	    callback=self.parse_page
			)