import streamlit as st
import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')
print(es.ping())

st.set_page_config(
    page_title="Nhóm 21",
    page_icon="👋",
)

app_mode_1 = st.sidebar.checkbox('Search Day')
app_mode_2 = st.sidebar.checkbox('Top 10 ngay tang nhieu nhat')
app_mode_3 = st.sidebar.checkbox('Top 10 ngay giam nhieu nhat')
app_mode_4 = st.sidebar.checkbox('Top 10 ngay giao dich nhieu nhat')
app_mode_5 = st.sidebar.checkbox('Top 10 ngay giao dich it nhat')

if app_mode_1:
    day_input = st.date_input('Day', value=None, min_value=datetime.date(1980, 12, 16), max_value=datetime.date(2023, 10, 30))
    if day_input != None:

        query1 = {
            'query': {
                'bool': {
                    'must': [
                        {'match_phrase': {'Date': day_input}}]}},
            'track_scores': True,
        }

        results = es.search(index='data_apple', body=query1)

        if results['hits']['total'] > 0:
            for res in results['hits']['hits']:
                st.markdown('Date: ' + str(res['_source']['Date']))
                st.markdown('Open: ' + str(res['_source']['Open']) + ' USD')
                st.markdown('High: ' + str(res['_source']['High']) + ' USD')
                st.markdown('Low: ' + str(res['_source']['Low']) + ' USD')
                st.markdown('Close: ' + str(res['_source']['Close']) + ' USD')
                st.markdown('Volume: ' + str(res['_source']['Volume']*1000000) + ' co phieu')
        else: 
            st.markdown("Ngay nay khong co trong Co So Du Lieu. Vui long chon ngay khac")

if app_mode_2:
    query2 = {
        'query': {
            "match_all": {}},
        'size': 10,
        'sort': [{'Change': {'order': 'desc'}}]
    }
    results = es.search(index='data_apple', body=query2)
    # st.write(results['hits']['total'])
    results = results['hits']['hits']
    st.subheader('Top 10 ngay tang nhieu nhat: ')
    for res in results:
        st.markdown(str(res['_source']['Date']) + ': ' + str(res['_source']['Change']) + ' %')
                
    # st.write(es.search(index='data_apple', body=query2))
    # st.write(results)

if app_mode_3:
    query3 = {
        'query': {
            "match_all": {}},
        'size': 10,
        'sort': [{'Change': {'order': 'asc'}}]
    }
    results = es.search(index='data_apple', body=query3)
    results = results['hits']['hits']
    st.subheader('Top 10 ngay giam nhieu nhat: ')
    for res in results:
        st.markdown(str(res['_source']['Date']) + ': ' + str(res['_source']['Change']) + ' %')
if app_mode_4:
    query4 = {
        'query': {
            "match_all": {}},
        'size': 10,
        'sort': [{'Volume': {'order': 'desc'}}]
    }
    results = es.search(index='data_apple', body=query4)
    results = results['hits']['hits']
    st.subheader('Top 10 ngay giao dich nhieu nhat: ')
    for res in results:
        st.markdown(str(res['_source']['Date']) + ': ' + str(res['_source']['Volume']) + ' co phieu')

if app_mode_5:
    query5 = {
        'query': {
            "match_all": {}},
        'size': 10,
        'sort': [{'Volume': {'order': 'asc'}}]
    }
    results = es.search(index='data_apple', body=query5)
    results = results['hits']['hits']
    st.subheader('Top 10 ngay giao dich it nhat: ')
    for res in results:
        st.markdown(str(res['_source']['Date']) + ': ' + str(res['_source']['Volume']) + ' co phieu')