# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
import json

class PostgreSQLPipeline:

    def  __init__(self):

        # Connection created
        self.connection = psycopg2.connect(host='postgres_container', user='postgres', password='postgres', dbname='postgres')

        # Connection cursor has beed established
        self.cur = self.connection.cursor()
        
        # If the table does exist we don't create a new one,
        # If not, we create a table named "raw_table"
        self.cur.execute("""CREATE TABLE IF NOT EXISTS raw_table(
        id serial PRIMARY KEY, 
        slug VARCHAR(255),
        language VARCHAR(255),
        languages TEXT,
        req_id VARCHAR(255),
        title TEXT,
        description TEXT,
        street_address TEXT,
        city VARCHAR(255),
        state VARCHAR(255),
        country_code VARCHAR(255),
        postal_code VARCHAR(255),
        location_type VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT,
        categories TEXT,
        tags TEXT,
        tags5 TEXT,
        tags6 TEXT,
        brand VARCHAR(255),
        promotion_value VARCHAR(255),
        salary_currency VARCHAR(255),
        salary_value VARCHAR(255),
        salary_min_value VARCHAR(255),
        salary_max_value VARCHAR(255),
        benefits TEXT,
        employment_type VARCHAR(255),
        hiring_organization TEXT,
        source VARCHAR(255),
        apply_url TEXT,
        internal BOOLEAN,
        searchable BOOLEAN,
        applyable BOOLEAN,
        li_easy_applyable BOOLEAN,
        ats_code VARCHAR(255),
        update_date TIMESTAMP,
        create_date TIMESTAMP,
        category VARCHAR(255),
        full_location VARCHAR(255),
        short_location VARCHAR(255)
        )
    """)

        # Then we commit our work to the postgresql service
        self.connection.commit()

    def process_item(self, item, spider):

        # We test out if there is a row 
        # with the same attribute, in this case it's "slug"
        self.cur.execute(f"SELECT * FROM raw_table WHERE slug = '{item.get('slug')}'")
        self.connection.commit()
        result = self.cur.fetchone()

        # If it is in database, we create log message
        if result:
            spider.logger.warn(f"Item already in database: {item.get('slug')}")
        
        # If not, we insert it into the table
        else:
            item_values = []
            for key in [
                    "slug", "language", "languages", "req_id", "title", "description",
                    "street_address", "city", "state", "country_code", "postal_code",
                    "location_type", "latitude", "longitude", "categories", "tags", "tags5",
                    "tags6", "brand", "promotion_value", "salary_currency", "salary_value",
                    "salary_min_value", "salary_max_value", "benefits", "employment_type",
                    "hiring_organization", "source", "apply_url", "internal", "searchable",
                    "applyable", "li_easy_applyable", "ats_code", "update_date",
                    "create_date", "category", "full_location", "short_location"
            ]:
                value = item.get(key)
                if value is None:
                    item_values.append('NULL')
                elif key in ["benefits", "languages", "category", "tags", "tags5", "tags6"]:
                    if (str(value) != "[]"):
                        item_values.append('ARRAY' + str(value))
                    else:
                        item_values.append('NULL')
                elif key == "description":
                    value = str(value).replace("'", "")
                    item_values.append("'" + value + "'")
                elif key == "categories":
                    categories_list = []
                    for ele in value:
                        categories_list.append(ele["name"].replace("'", ""))                    
                    item_values.append('ARRAY' + str(categories_list))
                elif type(value) == str:
                    value = str(value).replace("'", "")
                    item_values.append("'" + value + "'")
                else: 
                    item_values.append(value)

            # We insert a record to the table  
            self.cur.execute("""INSERT INTO raw_table (
                    slug, language, languages, req_id, title, description, street_address,
                    city, state, country_code, postal_code, location_type, latitude, longitude,
                    categories, tags, tags5, tags6, brand, promotion_value, salary_currency,
                    salary_value, salary_min_value, salary_max_value, benefits, employment_type,
                    hiring_organization, source, apply_url, internal, searchable, applyable,
                    li_easy_applyable, ats_code, update_date, create_date, category,
                    full_location, short_location) VALUES (
                            {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                            {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                            {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})""".format(*item_values))
            
            # Then we commit our work
            self.connection.commit()
                
            return item

    def close_spider(self, spider):

        # Close cursor & connection to database 
        self.cur.close()
        self.connection.close()