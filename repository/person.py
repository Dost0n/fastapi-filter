from config import db, commit_rollback
from model import Person
from schema import PersonCreate, PageResponse
from sqlalchemy.sql import select
from sqlalchemy import update, delete, or_, text, func, column
import math

class PersonRepository:
    
    @staticmethod
    async def create(create_form:PersonCreate):
        db.add(Person(
            name = create_form.name,
            sex = create_form.sex,
            birth_date = create_form.birth_date,
            birth_place = create_form.birth_place,
            country = create_form.country
        ))
        await commit_rollback()
        
    
    @staticmethod
    async def get_by_id(person_id:int):
        query = select(Person).where(Person.id == person_id)
        return (await db.execute(query)).scalar_one_or_none()
    
    
    @staticmethod
    async def update(person_id:int, update_form: PersonCreate):
        
        query = update(Person)\
            .where(Person.id == person_id)\
            .values(**update_form.dict())\
            .execution_options(synchronize_session="fetch")
        
        await db.execute(query)
        await commit_rollback()
        
    
    @staticmethod
    async def delete(person_id:int):
        query = delete(Person).where(Person.id == person_id)
        
        await db.execute(query)
        await commit_rollback()
        
    
    @staticmethod
    async def get_all(
        page: int = 1,
        limit: int = 0,
        columns: str = None,
        sort: str = None,
        filter: str = None
    ):
        query = select(from_obj=Person, columns="*")
        
        if columns is not None and columns != "all":
            query = select(from_obj=Person, columns=convert_columns(columns))
        
        if filter is not None and filter !="null":
            criteria = dict(x.split("*") for x in filter.split('-'))
            criteria_list = []
            for attr, value in criteria.items():
                _attr = getattr(Person, attr)
                
                search = "%{}%".format(value)
                
                criteria_list.append(_attr.like(search))
                
            query = query.filter(or_(*criteria_list))
            
        if sort is not None and sort!='null':
            query = query.order_by(text(convert_sort(sort)))
        
        offset_page = page-1
        query = (query.offset(offset_page*limit).limit(limit))
         
        count_query = select(func.count(1)).select_from(Person)
        
        total_record = (await db.execute(count_query)).scalar() or 0
        
        total_page = math.ceil(total_record/limit)
        
        result = (await db.execute(query)).fetchall()
        
        return PageResponse(page_number=page,
                            page_size=limit,
                            total_pages=total_page,
                            total_record=total_record,
                            content=result)


def convert_sort(sort):
    
    # split_sort = sort.split('-')
    # new_sort = ','.join(split_sort)
    return ','.join(sort.split('-'))

def convert_columns(columns):
    
    # new_columns = columns.split('-')
    # column_list = []
    # for data in new_columns:
    #     column_list.append(data)
    
    return list(map(lambda x: column(x), columns.split('-')))