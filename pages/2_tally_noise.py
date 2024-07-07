import pymysql
import pandas as pd
import json    
from datetime import datetime
import streamlit as st
import traceback
import os
from io import BytesIO
import openpyxl

st.set_page_config(page_title="Tally_noiselog")
class Noiselog:
    def __init__(self):
        try:
            self.env = None
            env = st.radio(
                '환경을 선택해주세요.',
                ('개발', '운영')
            )
            if env == '개발':
                
                # DB 접속s
                self.conn = pymysql.connect(host="DB접속 주소",
                        port=13306, user='사용자',
                        passwd='비밀번호',
                        db='DB',
                        charset='utf8')  # 한글처리 (charset = 'utf8')
                self.env = '개발'
                
                st.markdown("<h1 style='text-align: center; color: black;'>집계내역_noise_log(개발)</h1>", unsafe_allow_html=True)
            else:
                # DB 접속s
                self.conn = pymysql.connect(host="DB접속 주소",
                        port=13306, user='사용자',
                        passwd='비밀번호',
                        db='DB',
                        charset='utf8', cursorclass=pymysql.cursors.DictCursor)  # 한글처리 (charset = 'utf8')
                self.env = '운영'
                
                st.markdown("<h1 style='text-align: center; color: red;'>집계내역_noise_log(운영)</h1>", unsafe_allow_html=True)
            
            
            
            self.output = BytesIO()
            self.anls_dttm = None

            self.total = 0  # 총처리
            self.no_wav = 0  # 원본 없음
            self.sucess = 0  # 정상처리
            self.under = 0  # 원본 4초미만
            self.etc_err = 0  # 기타에러
            self.noise = 0  # 노이즈
            self.spectro_loss = 0  # 노이즈(spetro_loss)
            self.clear_confidence = 0  # 노이즈(clear_confidence)
            self.not_run = 0  # 미진행
            self.noise_success = 0
            self.gid = None
            self.now_date = str(datetime.today().strftime("%y_%m_%d"))
            self.total_list = []
            self.no_wav_list = []
            self.not_run_list = []
            self.anls_dttm_list = []
            self.sucess_list = []
            self.under_list = []
            self.noise_list = []
            self.spectro_loss_list = []
            self.clear_confidence_list = []
            self.etc_err_list = []
            self.noise_success_list = []

            self.anls_dttm_detl_list = []
            self.gid_list = []
            self.detl_text_list = []
            
            
            _, _, self.inquiry_col = st.columns([1, 8, 1])
            self.show_widget()
        except:
            st.error(traceback.format_exc())
    
    def show_widget(self):
        try:
            with self.inquiry_col:
                get_data_btn = st.button('조회')
            
            find_min_anls_dttm = self.get_min_anls_dttm()
            # 시작점 (최소) 날짜 찾기
            min_anls_dttm = min(find_min_anls_dttm['ANLS_DTTM'])
            
            # 원하는 형태로 변환
            begin = str(min_anls_dttm).split(' ')[0]
            end = str(datetime.today()).split(' ')[0]
            
            
            min_date = datetime(int(begin.split('-')[0]), int(begin.split('-')[1]), int(begin.split('-')[2]))
            max_date = datetime(int(end.split('-')[0]), int(end.split('-')[1]), int(end.split('-')[2]))
            get_date = st.date_input('날짜를 선택해주세요', (min_date, max_date))
            self.start_date = get_date[0]
            self.end_date = get_date[1]
           
            if get_date:
                df = self.get_df(begin=get_date[0], end=get_date[1])
                st.success(f'시작날짜 : {get_date[0]}  \n  끝 날짜 : {get_date[1]}')
                if get_data_btn:
                    
                    self.tally_process(df=df)
                    file_name, processed_data = self.make_tally_df()
                    tally, noise_success, noise_detl = st.tabs(['집계내역', '노이즈 정상처리 집계', '노이즈 세부 정보'])
                    with tally:
                        st.write(self.data_total_tally)
                    with noise_success:
                        st.write(self.data_noise_tally)
                    with noise_detl:
                        st.write(self.data_noise_detl)
                    st.download_button(label='Download to excel', data=processed_data, file_name=file_name)
         
        except IndexError:
            st.error('시작 날짜, 끝 날짜를 둘다 선택해주세요.')
        except:
            st.error(traceback.format_exc())
    def get_min_anls_dttm(self):
        try:
            cursor = self.conn.cursor()
            self.conn.ping(reconnect=True)
            query = 'SELECT ANLS_DTTM \
                        FROM TBD_L_CUST_REQ_ATP_DTA \
                        WHERE 1=1'
            cursor.execute(query)
            row = cursor.fetchall()
            self.conn.close()
            df = pd.DataFrame(row)
            return df
        except:
            st.error(traceback.format_exc())
            
    def get_df(self, begin, end):
        try:
            cursor = self.conn.cursor()
            self.conn.ping(reconnect=True)
            query = f"SELECT *\
                            FROM TBD_L_CUST_REQ_ATP_DTA dta \
                            INNER JOIN TBD_L_CUST_REQ_ATP_DTA_QLTY qlty \
                            ON dta.USR_MNGT_UNQ_NO = qlty.USR_MNGT_UNQ_NO \
                            AND dta.ANLS_REQ_SEQ = qlty.ANLS_REQ_SEQ \
                            WHERE 1=1 \
                            AND DATE_FORMAT(dta.ANLS_DTTM, '%Y-%m-%d') >= DATE_FORMAT('{begin}', '%Y-%m-%d') \
                            AND DATE_FORMAT(dta.ANLS_DTTM, '%Y-%m-%d') <= DATE_FORMAT('{end}', '%Y-%m-%d') \
                            ORDER BY dta.ANLS_DTTM"

            cursor.execute(query)
            row = cursor.fetchall()
            self.conn.close()
            df = pd.DataFrame(row)
            return df
        except:
            st.error(traceback.format_exc())
    
    def tally_process(self, df):
        for i in range(len(df)):
            # spectro_loss 값 리스트
            err_detl_list = []

            # 2021-12-24 11:53:06 → 2021-12-24
            self.anls_dttm = str(df.loc[i]['ANLS_DTTM']).split(' ')[0]

            # ------

            # 총 요청 개수 count
            self.total += 1

            # 엔진 음원 없음 확인
            if (df.loc[i]['AOSTC_RFNG_FILE_GID'] is None) and (df.loc[i]['STEMNG_FILE_GID'] is None) and df.loc[i]['QLTY_ANLS_STS_CODE'] == '10':
                self.no_wav += 1
            else:
                # 정상처리
                if df.loc[i]['FILTRG_ERR_DETL_CODE'] == '00':
                    # 정상처리중 미진행 건수 count
                    if df.loc[i]['QLTY_ANLS_STS_CODE'] == '10':
                        self.not_run += 1
                    # 정상 처리 건수 count
                    else:
                        self.sucess += 1

                # 원본 엔진음원 4초 미만 (기타오류)
                elif df.loc[i]['FILTRG_ERR_DETL_CODE'] == '10':
                    self.under += 1

                # 기타 에러
                elif df.loc[i]['FILTRG_ERR_DETL_CODE'] == '90':
                    self.etc_err += 1

                # 음원 노이즈
                elif (df.loc[i]['FILTRG_ERR_DETL_CODE'] == '20') or (df.loc[i]['FILTRG_ERR_DETL_CODE'] == '21') or (df.loc[i]['FILTRG_ERR_DETL_CODE'] == '22'):
                    if df.loc[i]['AOSTC_RFNG_MTHD_GB'] == 'M':
                        self.noise_success += 1
                    self.noise += 1
                    js = json.loads(df['FILTRG_ERR_DETL'][i])
                    data = js['data']
                    for n in range(len(data)):
                        
                        if df.loc[i]['AOSTC_RAW_FILE_GID'] == self.gid:
                            self.anls_dttm_detl_list.append('aabc')
                            self.gid_list.append('aabc')
                            data[n].pop('cos_similiarity')
                            self.detl_text_list.append(data[n])
                        else:
                            self.gid = df.loc[i]['AOSTC_RAW_FILE_GID']
                            self.anls_dttm_detl_list.append(self.anls_dttm)
                            self.gid_list.append(self.gid)
                            data[n].pop('cos_similiarity')
                            self.detl_text_list.append(data[n])
                            
                    for j in eval(df.loc[i]['FILTRG_ERR_DETL'])['data']:
                        # 노이즈 음원중 'spectro_loss' 값 err_detl_list에 append
                        err_detl_list.append(j['spectro_loss'])

                    # clear_confidence 값 할당
                    clear_confi = j['clear_confidence']
                    # spectro_loss 값과 clear_condfidence 값이 조건에 충족 시 count
                    if clear_confi == 1.0 and min(err_detl_list) >= 320:
                        self.spectro_loss += 1  # 차로 인식됬지만 잡음 많이 들어간 경우
                    else:
                        self.clear_confidence += 1  # 차로 인식이 안됨
                else:
                    if (df.loc[i]['AOSTC_RFNG_MTHD_GB'] != 'M') and (
                            df.loc[i]['FILTRG_ERR_DETL_CODE'] is None) and (
                            df.loc[i]['AOSTC_RFNG_FILE_GID'] is not None) and (
                            df.loc[i]['STEMNG_FILE_GID'] is not None):
                        self.sucess += 1


            if (i != len(df)-1 and self.anls_dttm == str(df.loc[i + 1]['ANLS_DTTM']).split(' ')[0]):
                # 현재 반복차수가 마지막 행이 아니면서
                # 다음 행과 비교하여 분석일자가 같을 경우
                pass
            else:
                # 다른
                # row 추가: anls_dttm 값이 다를 경우 기존 값 list에 append

                self.total_list.append(self.total)
                self.no_wav_list.append(self.no_wav)
                self.not_run_list.append(self.not_run)
                self.anls_dttm_list.append(self.anls_dttm)
                self.sucess_list.append(self.sucess)
                self.under_list.append(self.under)
                self.etc_err_list.append(self.etc_err)
                self.noise_list.append(self.noise)
                self.spectro_loss_list.append(self.spectro_loss)
                self.clear_confidence_list.append(self.clear_confidence)
                self.noise_success_list.append(self.noise_success)

                # 변수 초기화
                self.total = self.no_wav = self.not_run = self.sucess = self.under = self.etc_err = self.noise = self.spectro_loss = self.clear_confidence = self.noise_success = 0

    def make_tally_df(self):

        # # dataFrame 생성
        self.data_total_tally = pd.DataFrame({'분석일시': self.anls_dttm_list,
                            '총 요청': self.total_list,
                            '필터링 정상처리': self.sucess_list,
                            '미진행': self.not_run_list,
                            '원본음원 없음': self.no_wav_list,
                            '원본음원 4초 미만': self.under_list,
                            '노이즈(잡음이 많음)': self.spectro_loss_list,
                            '노이즈(자동차 아님)': self.clear_confidence_list,
                            '노이즈 합계': self.noise_list,
                            '기타오류': self.etc_err_list})

        self.data_noise_tally = pd.DataFrame({'분석일자' : self.anls_dttm_list,
                                        '노이즈 총 합계' : self.noise_list,
                                        '노이즈 정상처리' : self.noise_success_list})

        self.data_noise_detl = pd.DataFrame({'분석일자' : self.anls_dttm_detl_list,
                                        '원본 GID' : self.gid_list,
                                        '세부정보' : self.detl_text_list})

        self.data_noise_detl = self.data_noise_detl.replace('aabc', ' ')
        file_name = f'{self.now_date}_{self.env}_집계내역_노이즈로그{str(self.start_date)}_{str(self.end_date)}.xlsx'
        
        writer = pd.ExcelWriter(self.output, engine='openpyxl')

        self.data_total_tally.to_excel(writer, sheet_name='총 집계')
        self.data_noise_tally.to_excel(writer, sheet_name='노이즈 정상처리 집계')
        self.data_noise_detl.to_excel(writer, sheet_name='노이즈 데이터 세부정보')
        #result = data.to_json(orient='index')
        # data.to_excel('./집계내역_오늘날짜.xlsx')
        writer.save()
        processed_data = self.output.getvalue()
        return file_name, processed_data

if __name__ == '__main__':
    
    main = Noiselog()
    