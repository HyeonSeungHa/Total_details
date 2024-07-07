
from tkinter import CENTER
import traceback
import os
from turtle import color
import streamlit as st
import numpy as np
import matplotlib.pyplot as plt
import librosa
import librosa.display
import pymysql
import pandas as pd
import requests
from requests import get
import math
import glob
import uuid
import soundfile as sf
import time
import copy
import io
import urllib.request
from PIL import Image
import json
from requests_html import HTMLSession
# from pages import Noiselog
# from pages import reportsuccess
plt.rcParams['font.family'] = 'NanumGothic'

st.set_page_config(page_title="AuRRA")
class AuRRA:

    def __init__(self):
        try:
            self.mode = None
            
            env = st.radio(
                '환경을 선택해주세요.',
                ('개발', '운영')
            )
            if env == '개발':
                self.mode = 'dev'
                # DB 접속s
                self.conn = pymysql.connect(host="DB접속 주소",
                        port=13306, user='사용자',
                        passwd='비밀번호',
                        db='DB',
                        charset='utf8')  # 한글처리 (charset = 'utf8')
                
                # 관리자웹
                self.url = '음원파일을 다운 받는 주소'
                # wav_api
                self.url_api = '음원 편집 후 후처리 API 주소'

                # 고객 차량 정보 = Y 일때 보내는 api
                self.success_url = '작업 완료 후 보내는 주소'
                st.markdown("<h1 style='text-align: center; color: black;'>AuRRA_고객(개발)</h1>", unsafe_allow_html=True)
            else:
                self.mode = 'prod'
                
                # DB 접속s
                self.conn = pymysql.connect(host="DB접속 주소",
                        port=13306, user='사용자',
                        passwd='비밀번호',
                        db='DB',
                        charset='utf8')  # 한글처리 (charset = 'utf8')
                
                # 관리자웹
                self.url = '음원파일을 다운 받는 주소'

                # wav_api
                self.url_api = '음원 편집 후 후처리 API 주소'

                # 고객 차량 정보 = Y 일때 보내는 api
                self.success_url = '작업 완료 후 보내는 주소'
                st.markdown("<h1 style='text-align: center; color: red;'>AuRRA_고객(운영)</h1>", unsafe_allow_html=True)
            
            self.file_path = './file_dir/'
            os.makedirs(self.file_path, exist_ok=True)
            
            st.subheader(' ')
            self.columns = ['고객사 요청 관리 번호', '사용자 관리 고유 번호', '분석 요청 일련 번호', '정련 상태', '고객 명', '차량 번호', '개인 법인 구분',\
                       '분석 일시', '고객 차량 정보 최종 여부']
            self.usr_unq_no = 'U21091600001'
            self.usr_mngt_unq_no = None
            self.anls_req_seq = None
            self.carnum = None
            self.split_wav_gid = None
            self.cust_car_info = None
            
            self.state_feature_data = None
            self.state_get_img = None
            self.state_get_plain_img = None
            self.state_video_gid = None
            self.state_db_insert_dta = None
            self.state_db_insert_qlty = None 
            self.feature_data = None
            self.img_gid = None
            self.plain_img_gid = None
            self.video_gid = None
            self.ny = None
            self.split_path = None
            self.str_task_id = None
            self.get_data_success = None
            self.get_wav_img_success = None
            self.wav_load_success = None
            self.split_success = None
            self.call_api_success = None
            
            
            _, _, self.inquiry_col = st.columns([1, 8, 1])
            self.inquiry_tab, self.rawdata_tab, self.split_tab, self.db_insert_tab = st.tabs(['조회', '원본', '편집', 'DB적재'])
            self.show_widget()
            
        except:
            st.error(traceback.format_exc())
            return None
         
    # show widget
    def show_widget(self):
        try:
            
            with self.inquiry_tab:
                
                with self.inquiry_col:
                    get_data_btn = st.button('조회')

                self.index_text = st.text_input('인덱스를 입력해주세요 (숫자만 입력해주세요.)')
                
                df = self.get_data()

                if get_data_btn:
                    self.get_data_btn_click(df=df)
                    
                if self.index_text:
                    self.set_index_text_event(index_text=self.index_text, df=df)
                    
                    if self.get_data_success == True:
                        st.write(df)
                      
                    else:
                        pass
            with self.rawdata_tab:
                
                if self.get_wav_img_success == True:
                    fig, plain_fig = self.get_wav_img(self.audio_path)
                    st.pyplot(fig)
                    st.pyplot(plain_fig)
                else:
                    pass
                if self.wav_load_success == True:
                    audio_bytes = self.wav_load(audio_path=self.audio_path)
                    st.audio(audio_bytes, format='audio/wav')
                    start_sec_col, noise_col, etc_col = st.columns([5,1,1])
                    
                    with start_sec_col:
                        self.start_sec = st.text_input('시작점을 입력해주세요 (숫자만 입력해주세요.)')
                    
                    with noise_col:
                        
                        noise_btn = st.button('노이즈')
                        if noise_btn:
                            self.insert_data(code='10')
                            
                            
                    with etc_col:
                        
                        etc_btn = st.button('기타')
                        if etc_btn:
                            self.insert_data(code='90')
                            
                    if self.start_sec:
                        if self.start_sec == '':
                            pass
                        else:
                            
                            self.split_path = self.splithandler(self.start_sec)
                            
            with self.split_tab:
                
                if self.split_path is not None:
                    
                    st.markdown("<h4 style='text-align: center; color: black;'>편집 음원 시각화 이미지</h4>", unsafe_allow_html=True)
                    st.subheader(' ')
                    split_fig, split_plain_fig = self.get_wav_img(self.split_path)
                    st.pyplot(split_fig)
                    st.pyplot(split_plain_fig)
                    split_audio_bytes = self.wav_load(self.split_path)
                    st.audio(split_audio_bytes, format='audio/wav')
                    self.split_success = True
            
            with self.db_insert_tab:
                
                if self.split_success == True:
                    _, api_call_btn_col = st.columns([7, 1])
                    
                    with api_call_btn_col:
                        
                        api_call_btn = st.button('API호출 & DB저장')
                        if api_call_btn:
                           # st.session_state['call_cnt'] += 1
                           
                           with st.spinner('처리중'):
                                self.load_api_data()
                                if self.call_api_success == True:
                                    self.db_insert()
                                else:
                                    pass
                        else:
                            pass
                    st.success('특징 값 추출 : {}  \n  기본 이미지 : {}  \n  플레인 이미지 : {}  \n  비디오 : {}  \n  \n  데이터 저장(고객 요청 비정형 데이터 내역) : {}  \n  데이터 저장(고객 요청 비정형 데이터 품질 내역) : {}'.format(self.state_feature_data, self.state_get_img, self.state_get_plain_img, self.state_video_gid, self.state_db_insert_dta, self.state_db_insert_qlty))       
                            
        except RuntimeError:
            st.error('음원 오류')
            
        except:
            print(traceback.format_exc())
            
    # Clear
    def clear(self):
        try:
            self.get_data_success = None
            self.get_wav_img_success = None
            self.wav_load_success = None
            self.index_text = ''
            self.start_sec = ''
        except:
            st.error(traceback.format_exc())
            
    # 조회 버튼 클릭 이벤트
    def get_data_btn_click(self, df):
        try:
            if df is None:
                st.write("DF 가져오기 에러")
                pass
            else:
                
                st.write(df)
                
        except:
            st.error(traceback.format_exc())        
                
    # 인덱스 값 입력 이벤트
    def set_index_text_event(self, index_text, df):
        try:
            if df is None:
                pass
            else:
                self.get_data_success = True
                index_num = int(index_text)
                        
                self.carnum = df.iloc[index_num][5]
                self.usr_mngt_unq_no = df.iloc[index_num][1]
                self.anls_req_seq = df.iloc[index_num][2]
                self.cust_car_info = df.iloc[index_num][8]
                gid = self.get_gid(usr_no=self.usr_mngt_unq_no, anls_seq=self.anls_req_seq)
                if gid is None:
                    st.wrtie('GID 가져오기 에러')
                else:
                    download_url = f'{self.url}/fileManager/download?fileId=%s' % (gid[0])
                    task_id = uuid.uuid1()
                    self.str_task_id = str(task_id)
                    
                    if self.download(url=download_url, file_name=self.file_path + self.str_task_id + '_' + self.carnum + '.wav') is None:
                        st.write('다운로드 실패')
                    else:
                        self.audio_path = f'{self.file_path}{self.str_task_id}_{self.carnum}.wav'
                        

                        if self.get_wav_img(audio_path=self.audio_path) is None:
                            st.write('이미지 가져오기 에러')
                        else:
                            self.get_wav_img_success = True
                            
                            if self.wav_load(audio_path=self.audio_path) is None:
                                st.write('음원 가져오기 에러')
                            else:
                                self.wav_load_success = True
                                
        except ValueError:
            st.error('숫자만 입력해주세요.')
            return None
        except:
            st.error(traceback.format_exc())
            return None        
              
    # 데이터 조회 및 데이터프레임 생성
    def get_data(self):
        try:
            cur = self.conn.cursor()
            self.conn.ping(reconnect=True)
            sql = f"SELECT \
                    req.CLNTCO_REQ_MNGT_NO, \
                    req.USR_MNGT_UNQ_NO, \
                    req.ANLS_REQ_SEQ, \
                        CASE \
                            WHEN qlty.AOSTC_RAW_FILE_GID IS NOT NULL AND qlty.AOSTC_RFNG_FILE_GID IS NOT NULL \
                            THEN '정련 완료' \
                            WHEN qlty.AOSTC_RAW_FILE_GID IS NOT NULL AND qlty.AOSTC_RFNG_FILE_GID IS NULL AND req.QLTY_ANLS_RSLT_CODE IS NULL\
                            THEN '정련 미완료' \
                            WHEN qlty.AOSTC_RAW_FILE_GID IS NULL AND req.QLTY_ANLS_RSLT_CODE IS NULL\
                            THEN '원본 없음' \
                            WHEN req.QLTY_ANLS_RSLT_CODE != '00'\
                            THEN '정련 불가' \
                        END AS AOSTC_RFNG_PRCS_GB, \
                    cusr.CUST_NM,\
                    carnum.CAR_REG_NO,\
                    (SELECT splc.SPLC_NM FROM TAC_M_SPLC_INFO splc\
                        WHERE splc.SPLC_MNGT_UNQ_NO = cusr.SPLC_MNGT_UNQ_NO) AS SPLC_NM,\
                    req.ANLS_DTTM,\
                    req.CUST_CAR_INFO_LST_YN\
                    FROM TBD_L_CUST_REQ_ATP_DTA req  \
                    INNER JOIN TBD_L_CUST_REQ_ATP_DTA_QLTY qlty ON req.USR_MNGT_UNQ_NO = qlty.USR_MNGT_UNQ_NO \
                    AND req.ANLS_REQ_SEQ = qlty.ANLS_REQ_SEQ \
                    LEFT OUTER JOIN ( \
                    SELECT \
                      usr.USR_MNGT_UNQ_NO, \
                      usr.USR_NM, \
                      usr.CUST_MNGT_UNQ_NO, \
                      cust.CUST_NM, \
                      cust.PSNL_CRP_GB, \
                      cust.SPLC_MNGT_UNQ_NO \
                    FROM TAA_M_USER_INFO usr \
                    INNER JOIN TBA_M_CUST_INFO cust ON usr.CUST_MNGT_UNQ_NO = cust.CUST_MNGT_UNQ_NO \
                    ) cusr \
                    ON req.USR_MNGT_UNQ_NO = cusr.USR_MNGT_UNQ_NO \
                    LEFT OUTER JOIN TAB_L_CAR_PRDT_LIST carnum \
                    ON carnum.CAR_PRDT_MNGT_NO = req.CAR_PRDT_MNGT_NO\
                    ORDER BY req.ANLS_DTTM DESC"
            
            cur.execute(sql)
            query_result = cur.fetchall()
            df = pd.DataFrame(query_result, columns=self.columns)
            self.conn.close()
            return df

        except:
            st.error(traceback.format_exc())
            return None
    
    # API 호출
    def call_api(self, gid, code):
        try:
            
            # 특징 값 호출
            if code == '00':
                
                
                r = requests.get(f'{self.url_api}/extract_features/?gid={gid}&mode={self.mode}')
                
                time.sleep(1)
                if r.json()['task_id'] is not None:
                    task_id = r.json()['task_id']
                    while True:
                        time.sleep(0.5)
                        res = requests.get(f'{self.url_api}/extract_features/?gid={gid}&req_task_id={task_id}&mode={self.mode}').json()
                        if 'detail' in res:
                            pass
                        else:
                            if ('msg' not in res) and ('task_id' not in res):
                                break
                            else:
                                print('특징값 : ', res['message'])
                    
                    if res['data']['zero_crossing_rate_mean'] is not None:
                        self.state_feature_data = '정상처리'
                        self.feature_data = res['data']
                        print(self.feature_data)
                    else:
                        self.state_feature_data = '오류'
            
            # 시각화 이미지 생성
            elif code == '10':
                
                r = requests.get(f'{self.url_api}/extract_spectro_png/?gid={gid}&mode={self.mode}')
                task_id = r.json()['task_id']
                
                if task_id is not None:
                    while True:
                        time.sleep(0.5)
                        img_url = requests.get(f'{self.url_api}/extract_spectro_png/?gid={gid}&req_task_id={task_id}&mode={self.mode}')
                        if 'detail' in str(img_url.content):
                            pass
                        else:
                            if ('msg' not in str(img_url.content)) and ('task_id' not in str(img_url.content)):
                                break
                            else:
                                print('시각화 이미지 : ', img_url.content['msg'])
                else:
                    pass
                img_path = self.file_path + self.str_task_id + '_split_' + self.carnum + '.png'
                with open(img_path, 'wb') as f:
                    f.write(img_url.content)
                    f.close()
                img_file = open(img_path, 'rb')
                upload_file = {'file' : img_file}
                key_data = {'key' : 'CUST_WAV_SPECTRO_PNG'}
                
                res = requests.post(f'{self.url}/fileManager/upload', files=upload_file, data=key_data).json()
                if res['fileGid'] is not None:
                    self.state_get_img = '정상처리'
                    self.img_gid = res['fileGid']
                    print(self.img_gid)
                else:
                    self.state_get_img = '오류'
                
            
            # 플레인 시각화 이미지 생성
            elif code =='20':
                r = requests.get(f'{self.url_api}/extract_spectro_hz/?gid={gid}&mode={self.mode}')
                task_id = r.json()['task_id']
                
                if task_id is not None:
                    while True:
                        time.sleep(0.5)
                        plain_img_url = requests.get(f'{self.url_api}/extract_spectro_hz/?gid={gid}&req_task_id={task_id}&mode={self.mode}')
                        if 'detail' in str(plain_img_url.content):
                            pass
                        else:
                            if ('msg' not in str(plain_img_url.content)) and ('task_id' not in str(plain_img_url.content)):
                                break
                            else:
                                print('플레인 시각화 : ', plain_img_url.content['msg'])
                else:
                    pass
                
                plain_img_path = self.file_path + self.str_task_id + '_split_plain_' + self.carnum + '.png'
                with open(plain_img_path, 'wb') as f:
                    f.write(plain_img_url.content)
                    f.close()
                plain_img_file = open(plain_img_path, 'rb')
                upload_file = {'file' : plain_img_file}
                key_data = {'key' : 'CUST_WAV_SPECTRO_PNG'}
                
                res = requests.post(f'{self.url}/fileManager/upload', files=upload_file, data=key_data).json()
                if res['fileGid'] is not None:
                    self.state_get_plain_img = '정상처리'
                    self.plain_img_gid = res['fileGid']
                    print(self.plain_img_gid)
                else:
                    self.state_get_plain_img = '오류'
                
            
            # video 파일 생성
            elif code == '30':
                r = requests.get(f'{self.url_api}/get_spectro_mp4/?gid={gid}&mode={self.mode}')
                task_id = r.json()['task_id']
                
                if task_id is not None:
                    while True:
                        time.sleep(0.5)
                        video_url = requests.get(f'{self.url_api}/get_spectro_mp4/?gid={gid}&req_task_id={task_id}&mode={self.mode}')
                        if 'detail' in str(video_url.content):
                            pass
                        else:
                            if ('message' not in str(video_url.content)) and ('task_id' not in str(video_url.content)):
                                break
                            else:
                                res_dict = json.loads(video_url.content.decode('utf-8'))
                                print('mp4 : ', res_dict['message'])
                    else:
                        pass    
                else:
                    pass
                
                
                video_path = self.file_path + self.str_task_id + '_split_video_' + self.carnum + '.mp4'
                
                with open(video_path, 'wb') as f:
                    f.write(video_url.content)
                    f.close()
                    
                video_file = open(video_path, 'rb')
                upload_file = {'file' : video_file}
                key_data = {'key' : 'CUST_WAV_SPECTRO_MP4'}
                
                res = requests.post(f'{self.url}/fileManager/upload', files=upload_file, data=key_data).json()
                
                if res['fileGid'] is not None:
                    self.state_video_gid = '정상처리'
                    self.video_gid = res['fileGid']
                    print(self.video_gid)
                    
                else:
                    self.state_vide_gid = '오류'
                
            else:
                st.error('코드를 정확히 입력해주세요')
                pass
                
        except:
            print(traceback.format_exc())
            
    # 차량 수집 세부내역 insert
    def insert_data(self, code):
        try:
            cur = self.conn.cursor()
            self.conn.ping(reconnect=True)
            if code is None:
                st.error('코드 없음(정상처리, 노이즈, 기타)')
                
            else:
                if code == '00':
                    remark = f'시작점 : {self.start_sec}초  \n  끝점 : {int(self.start_sec) + 4}초'
                    aostc_rfng_cln_yn = 'Y'
                    aostc_rfng_mthd_gb = 'M'
                    qlty_anls_rslt_code = '00'
                    sql = f"UPDATE TBD_L_CUST_REQ_ATP_DTA SET\
                                ANLS_DTTM = NOW(),\
                                AOSTC_RFNG_CLN_YN = '{aostc_rfng_cln_yn}',\
                                AOSTC_RFNG_CLN_DTTM = NOW(),\
                                AOSTC_RFNG_MTHD_GB = '{aostc_rfng_mthd_gb}',\
                                RFNG_PRCS_USR_MNGT_UNQ_NO = '{self.usr_unq_no}',\
                                QLTY_ANLS_RSLT_CODE = '{qlty_anls_rslt_code}',\
                                REMARK = '{remark}',\
                                LST_USR_MNGT_UNQ_NO = '{self.usr_unq_no}',\
                                LST_CHNG_DTTM = NOW()\
                            WHERE USR_MNGT_UNQ_NO = '{self.usr_mngt_unq_no}' AND ANLS_REQ_SEQ = '{self.anls_req_seq}'"
                            
                                
                else:
                        
                    if code == '10':
                        remark = '결과 : 노이즈'
                        aostc_rfng_cln_yn = 'N'
                        qlty_anls_rslt_code = '10'
                        
                        
                        
                    elif code == '90':
                        remark = '결과 : 원본4초미만'
                        aostc_rfng_cln_yn = 'N'
                        qlty_anls_rslt_code = '90'
                        
                    else:
                        pass
                    sql = f"UPDATE TBD_L_CUST_REQ_ATP_DTA SET \
                                AOSTC_RFNG_CLN_YN = '{aostc_rfng_cln_yn}',\
                                QLTY_ANLS_RSLT_CODE = '{qlty_anls_rslt_code}',\
                                REMARK = '{remark}',\
                                LST_USR_MNGT_UNQ_NO = '{self.usr_unq_no}',\
                                LST_CHNG_DTTM = NOW() \
                            WHERE USR_MNGT_UNQ_NO = '{self.usr_mngt_unq_no}' AND ANLS_REQ_SEQ = '{self.anls_req_seq}'"
                
                cur.execute(sql)
                self.conn.commit()
                self.conn.close()
                
                if code != '00':
                    st.error('편집불가')
                    time.sleep(1)
                    
                    
                    for file in glob.glob(f'{self.file_path}*{self.carnum}*'):
                        os.remove(file)
                    
                else:
                    self.state_db_insert_dta = True
                    time.sleep(1)
                    
                    
            
        except pymysql.err.OperationalError:
            st.error('DB연결을 확인해주세요.')
            st.error(traceback.format_exc())
        
        except pymysql.err.InterfaceError:
            st.error('DB연결을 확인해주세요.')
            st.error(traceback.format_exc())
            
        except pymysql.err.DataError:  
            st.error('데이터 세부정보 저장 실패 : 데이터를 확인해주세요.')
            st.error(traceback.format_exc())
        
        except:
            print(traceback.format_exc())
    
    # load api data
    def load_api_data(self):
        try:
            self.call_api(gid=self.split_wav_gid, code='00') # 특징값
            time.sleep(0.5)
            self.call_api(gid=self.split_wav_gid, code='10') # 시각화
            time.sleep(0.5)
            self.call_api(gid=self.split_wav_gid, code='20') # 시각화 플레인
            time.sleep(0.5)
            self.call_api(gid=self.split_wav_gid, code='30') # mp4 파일
            if self.feature_data['length'] is not None:
                self.call_api_success = True
            else:
                pass
        except:
            st.error(traceback.format_exc())
            
    # 특징 값 insert
    def insert_feature_data(self):
        try:
            
            cur = self.conn.cursor()
            self.conn.ping(reconnect=True)
            sql = f"UPDATE TBD_L_CUST_REQ_ATP_DTA_QLTY SET\
                        USR_MNGT_UNQ_NO = '{self.usr_mngt_unq_no}',\
                        ANLS_REQ_SEQ = '{self.anls_req_seq}',\
                        AOSTC_RFNG_FILE_GID = '{self.split_wav_gid}',\
                        LENGTH = '{self.feature_data['length']}',\
                        LENGTH_SEC = '{self.feature_data['length_sec']}',\
                        CHROMA_STFT_MEAN = '{self.feature_data['chroma_stft_mean']}',\
                        CHROMA_STFT_VAR = '{self.feature_data['chroma_stft_var']}',\
                        RMS_MEAN = '{self.feature_data['rms_mean']}',\
                        RMS_VAR = '{self.feature_data['rms_var']}',\
                        SPECTRAL_CENTROID_MEAN = '{self.feature_data['spectral_centroid_mean']}',\
                        SPECTRAL_CENTROID_VAR = '{self.feature_data['spectral_centroid_var']}',\
                        SPECTRAL_BANDWIDTH_MEAN = '{self.feature_data['spectral_bandwidth_mean']}',\
                        SPECTRAL_BANDWIDTH_VAR = '{self.feature_data['spectral_bandwidth_var']}',\
                        ROLLOFF_MEAN = '{self.feature_data['rolloff_mean']}',\
                        ROLLOFF_VAR = '{self.feature_data['rolloff_var']}',\
                        ZERO_CROSSING_RATE_MEAN = '{self.feature_data['zero_crossing_rate_mean']}',\
                        ZERO_CROSSING_RATE_VAR = '{self.feature_data['zero_crossing_rate_var']}',\
                        HARMONY_MEAN = '{self.feature_data['harmony_mean']}',\
                        HARMONY_VAR = '{self.feature_data['harmony_var']}',\
                        PERCEPTR_MEAN = '{self.feature_data['perceptr_mean']}',\
                        PERCEPTR_VAR = '{self.feature_data['perceptr_var']}',\
                        TEMPO = '{self.feature_data['tempo']}',\
                        MFCC1_MEAN = '{self.feature_data['mfcc0_mean']}',\
                        MFCC1_VAR = '{self.feature_data['mfcc0_var']}',\
                        MFCC2_MEAN = '{self.feature_data['mfcc1_mean']}',\
                        MFCC2_VAR = '{self.feature_data['mfcc1_var']}',\
                        MFCC3_MEAN = '{self.feature_data['mfcc2_mean']}',\
                        MFCC3_VAR = '{self.feature_data['mfcc2_var']}',\
                        MFCC4_MEAN = '{self.feature_data['mfcc3_mean']}',\
                        MFCC4_VAR = '{self.feature_data['mfcc3_var']}',\
                        MFCC5_MEAN = '{self.feature_data['mfcc4_mean']}',\
                        MFCC5_VAR = '{self.feature_data['mfcc4_var']}',\
                        MFCC6_MEAN = '{self.feature_data['mfcc5_mean']}',\
                        MFCC6_VAR = '{self.feature_data['mfcc5_var']}',\
                        MFCC7_MEAN = '{self.feature_data['mfcc6_mean']}',\
                        MFCC7_VAR = '{self.feature_data['mfcc6_var']}',\
                        MFCC8_MEAN = '{self.feature_data['mfcc7_mean']}',\
                        MFCC8_VAR = '{self.feature_data['mfcc7_var']}',\
                        MFCC9_MEAN = '{self.feature_data['mfcc8_mean']}',\
                        MFCC9_VAR = '{self.feature_data['mfcc8_var']}',\
                        MFCC10_MEAN = '{self.feature_data['mfcc9_mean']}',\
                        MFCC10_VAR = '{self.feature_data['mfcc9_var']}',\
                        MFCC11_MEAN = '{self.feature_data['mfcc10_mean']}',\
                        MFCC11_VAR = '{self.feature_data['mfcc10_var']}',\
                        MFCC12_MEAN = '{self.feature_data['mfcc11_mean']}',\
                        MFCC12_VAR = '{self.feature_data['mfcc11_var']}',\
                        MFCC13_MEAN = '{self.feature_data['mfcc12_mean']}',\
                        MFCC13_VAR = '{self.feature_data['mfcc12_var']}',\
                        MFCC14_MEAN = '{self.feature_data['mfcc13_mean']}',\
                        MFCC14_VAR = '{self.feature_data['mfcc13_var']}',\
                        MFCC15_MEAN = '{self.feature_data['mfcc14_mean']}',\
                        MFCC15_VAR = '{self.feature_data['mfcc14_var']}',\
                        MFCC16_MEAN = '{self.feature_data['mfcc15_mean']}',\
                        MFCC16_VAR = '{self.feature_data['mfcc15_var']}',\
                        MFCC17_MEAN = '{self.feature_data['mfcc16_mean']}',\
                        MFCC17_VAR = '{self.feature_data['mfcc16_var']}',\
                        MFCC18_MEAN = '{self.feature_data['mfcc17_mean']}',\
                        MFCC18_VAR = '{self.feature_data['mfcc17_var']}',\
                        MFCC19_MEAN = '{self.feature_data['mfcc18_mean']}',\
                        MFCC19_VAR = '{self.feature_data['mfcc18_var']}',\
                        MFCC20_MEAN = '{self.feature_data['mfcc19_mean']}',\
                        MFCC20_VAR = '{self.feature_data['mfcc19_var']}',\
                        VSLZ_FILE_GID = '{self.img_gid}',\
                        VSLZ_PLAIN_FILE_GID = '{self.plain_img_gid}', \
                        VSLZ_STRMVDO_FILE_GID = '{self.video_gid}', \
                        FST_USR_MNGT_UNQ_NO = '{self.usr_unq_no}',\
                        FST_CRT_DTTM = NOW(),\
                        LST_USR_MNGT_UNQ_NO = '{self.usr_unq_no}',\
                        LST_CHNG_DTTM = NOW() \
                    WHERE USR_MNGT_UNQ_NO = '{self.usr_mngt_unq_no}' AND ANLS_REQ_SEQ = '{self.anls_req_seq}'"
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
            self.state_db_insert_qlty = True
            try:
                if self.cust_car_info == 'Y':
                    headers = {'SVCKEY': 'OCMOBILE','Content-Type': 'application/json'}
                    data = {'usr_mngt_unq_no': f'{self.usr_mngt_unq_no}', 'anls_req_seq': f'{self.anls_req_seq}'}
                    
                    res = requests.post(self.success_url, headers=headers, data=json.dumps(data)).json()
                    
                    time.sleep(1)
                    return st.success('코드 : {}  \n  메세지 : {}'.format(res['result_code'], res['result_msg']))
                else:
                    st.success('고객 차량 최종 여부 : {}'.format(self.cust_car_info))
                for file in glob.glob(f'{self.file_path}*{self.carnum}*'):
                    os.remove(file)
            except:
                st.error(traceback.format_exc())
        except:
            print(traceback.format_exc())
    
    # DB insert
    def db_insert(self):
        try:
            self.insert_data(code='00')
            time.sleep(1)
            self.insert_feature_data()
        except:
            st.error(traceback.format_exc())    
                
    # GID 가져오기
    def get_gid(self, usr_no, anls_seq):
        try:
            cur = self.conn.cursor()
            self.conn.ping(reconnect=True)

            sql = f'SELECT AOSTC_RAW_FILE_GID \
                    FROM TBD_L_CUST_REQ_ATP_DTA_QLTY \
                    WHERE USR_MNGT_UNQ_NO = "{usr_no}" \
                    AND ANLS_REQ_SEQ = "{anls_seq}"'
            cur.execute(sql)
            row_gid = cur.fetchall()

            self.conn.close()
            return row_gid
        except:
            st.error(traceback.format_exc())
            return None
        
    # 파일 다운로드
    def download(self, url, file_name):
        try:
            with open(file_name, "wb") as file:
                response = get(url)
                file.write(response.content)
                file_size = os.path.getsize(file_name)
                convert_size = self.convert_size(file_size)
                if convert_size is None:
                    st.wrtie('음원 사이즈 변환 실패')
                else:
                    return st.success('다운로드 완료 : {}  \n  용량 : {} bytes'.format(self.carnum, convert_size))
        except:
            st.error(traceback.format_exc())
            return None
        
    # 시각화 이미지 생성
    def get_wav_img(self, audio_path):
        try:
            (_, file_id) = os.path.split(audio_path)
            self.y, self.sr = librosa.load(audio_path)
            
            time = np.linspace(0, len(self.y) / self.sr, len(self.y))
            
            fig, ax = plt.subplots()
            ax.plot(time, self.y, color='b', label='speech waveform')
            ax.set_ylabel("Amplitude")
            ax.set_xlabel("Time [s]")
            ax.set_title(self.carnum)
            
            plain_fig, plain_ax = plt.subplots()
            
            hop_length = 512
            window_size = 1024
            
            window = np.hanning(window_size)
            stft = librosa.core.spectrum.stft(self.y, n_fft=window_size, hop_length=512, window=window)
            out = 2 * np.abs(stft) / np.sum(window)
            data = librosa.amplitude_to_db(out, ref=np.max)

            img = librosa.display.specshow(data, sr=self.sr, hop_length=hop_length, x_axis='time',
                                           y_axis='hz', cmap='viridis')
            self.raw_colorbar = plain_fig.colorbar(img, ax=plain_ax, format='%+2.0f dB')
            plain_ax.set_ylabel("Frequency")
            plain_ax.set_xlabel("Time")
            plain_ax.set_title(self.carnum)
            
            return fig, plain_fig
        except EOFError:
            st.error('음원 오류')
            
        except:
            st.write(traceback.format_exc())
            
        
    # 음원 불러오기
    def wav_load(self, audio_path):
        try:
            audio_file = open(audio_path, 'rb')
            audio_bytes = audio_file.read()
            
            return audio_bytes
        
        
            
        except:
            st.error(traceback.format_exc())
            
        
    # 파일 사이즈 변환
    def convert_size(self,size_bytes):
        try:
            if size_bytes == 0:
                return "0B"
            size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
            i = int(math.floor(math.log(size_bytes, 1024)))
            p = math.pow(1024, i)
            s = round(size_bytes / p, 2)
            return "%s %s" % (s, size_name[i])
        except:
            st.write(traceback.format_exc())
            return None
     
    # 시작점(초) 가져오기 및 편집
    def splithandler(self, sec):
        try:
            ny = self.y[int(sec) * self.sr : (int(sec) + 4) * self.sr]
            sf.write(self.file_path + self.str_task_id + '_split_' + self.carnum + '.wav', ny, self.sr)
            split_path = self.file_path + self.str_task_id + '_split_' + self.carnum + '.wav'
            
            wav_file = open(split_path, 'rb')
            upload_file = {'file' : wav_file}
            key_data = {'key' : 'CUST_REQ_ATP_DTA'}
            
            res = requests.post(f'{self.url}/fileManager/upload', files=upload_file, data=key_data).json()
            self.split_wav_gid = res['fileGid']
            
            return split_path
        
        except ValueError:
            st.error('숫자만 입력해주세요.')
            return None
        
        except:
            st.error(traceback.format_exc())
            return None 

if __name__ == '__main__':
    
    main = AuRRA()
    
