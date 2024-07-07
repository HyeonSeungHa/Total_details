import pymysql
import pandas as pd
import json    
from datetime import datetime
import streamlit as st
import traceback
import os
import openpyxl

from io import BytesIO
st.set_page_config(page_title="Tally_report_success")
class reportsuccess:
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
                        charset='utf8', cursorclass=pymysql.cursors.DictCursor)  # 한글처리 (charset = 'utf8')
                self.env = '개발'
                
                st.markdown("<h1 style='text-align: center; color: black;'>집계내역_보고서생성여부(개발)</h1>", unsafe_allow_html=True)
            else:
                # DB 접속s
                self.conn = pymysql.connect(host="DB접속 주소",
                        port=13306, user='사용자',
                        passwd='비밀번호',
                        db='DB',
                        charset='utf8', cursorclass=pymysql.cursors.DictCursor)  # 한글처리 (charset = 'utf8')
                self.env = '운영'
                
                st.markdown("<h1 style='text-align: center; color: red;'>집계내역_보고서생성여부(운영)</h1>", unsafe_allow_html=True)

            self.output = BytesIO()
            self.anls_dttm = None

            self.total = 0  # 총처리

            self.filter_re_success = 0  # 정상처리
            self.filter_re_fail = 0
            self.no_wav = 0  # 원본 없음
            self.filter_noise = 0  # 노이즈
            self.filter_etc_err = 0  # 기타에러

            self.fail_total = 0
            self.noise_etc_total = 0


            self.manual_re_success = 0
            self.manual_re_fail = 0
            self.manual_noise = 0
            self.manual_etc_err = 0
            
            self.gid = None

            self.total_list = []
            self.no_wav_list = []
            self.anls_dttm_list = []
            self.filter_re_success_list = []
            self.filter_re_fail_list = []
            self.filter_noise_list = []
            self.filter_etc_err_list = []
            self.noise_etc_total_list = []
            self.manual_re_success_list = []
            self.manual_re_fail_list = []
            self.manual_noise_list = []
            self.manual_etc_err_list = []
            self.now_date = str(datetime.today().strftime("%y_%m_%d"))
            
            

            self.col = ['분석일자', '고객사 요청 관리 고유번호', '차량 등록 번호', '차량정보', '차량 브랜드 명', '차량 분류 모델 명', '차량 모델 명', 
                    '차량 모델 등급 명', 'MID_RANGE_SIGNAL_합계', 'LOW_HIGH_RANGE_SIGNAL_합계', 'PERIODIC_SIGNAL_합계', 'APERIODIC_SIGNAL_합계',
                    'HUMAN_FRIENDLY_SIGNAL_합계', '합계 점수']
            
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
                    report_detl_df = self.report_detl(begin=get_date[0], end=get_date[1])
                    file_name, processed_data, total_tally = self.make_tally_df(report_df=report_detl_df)
                    tally, report_detl = st.tabs(['집계내역', '상세 개별 내역'])
                    with tally:
                        st.write(total_tally)
                    with report_detl:
                        st.write(report_detl_df)
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
    
    def tally_process(self, df):
        for i in range(len(df)):
            # spectro_loss 값 리스트
            # err_detl_list = []

            # 2021-12-24 11:53:06 → 2021-12-24
            self.anls_dttm = str(df.loc[i]['ANLS_DTTM']).split(' ')[0]

            # ------

            # 총 요청 개수 count
            self.total += 1

            # 엔진 음원 없음 확인
            if (df.loc[i]['AOSTC_RFNG_FILE_GID'] is None and df.loc[i]['STEMNG_FILE_GID'] is None):
                self.no_wav += 1
            else:
                # 필터링 엔진 레포트 o
                if (df.loc[i]['INIT_FILTRG_PRCS_GB'] == '~' or df.loc[i]['INIT_FILTRG_PRCS_GB'] == '00') and df.loc[i]['QLTY_ANLS_STS_CODE'] == '00' and df.loc[i]['AOSTC_RFNG_MTHD_GB'] != 'M':
                    # 정상처리중 미진행 건수 count
        
                    # 정상 처리 건수 count
                    
                    # 필터링 엔진 O
                    self.filter_re_success += 1

                # 필터링 엔진 레포트 x
                elif (df.loc[i]['INIT_FILTRG_PRCS_GB'] == '~' or df.loc[i]['INIT_FILTRG_PRCS_GB'] == '00') and df.loc[i]['QLTY_ANLS_STS_CODE'] == '10' and df.loc[i]['AOSTC_RFNG_MTHD_GB'] != 'M':
                    self.filter_re_fail += 1

                # 필터링 엔진 노이즈
                elif df.loc[i]['FILTRG_ERR_DETL_CODE'] == '20' or df.loc[i]['FILTRG_ERR_DETL_CODE'] == '21' or df.loc[i]['FILTRG_ERR_DETL_CODE'] == '22':
                    self.filter_noise += 1
                    # 수작업 레포트 O
                    if df.loc[i]['AOSTC_RFNG_MTHD_GB'] == 'M' and df.loc[i]['AI_CPBLTCU_ISU_YN'] == 'Y' and df.loc[i]['QLTY_ANLS_RSLT_CODE'] == '00':
                        self.manual_re_success += 1
                        
                    # 수작업 레포트 X
                    elif df.loc[i]['AOSTC_RFNG_MTHD_GB'] == 'M' and df.loc[i]['AI_CPBLTCU_ISU_YN'] == 'N' and df.loc[i]['QLTY_ANLS_RSLT_CODE'] == '00':
                        self.manual_re_fail += 1
                        
                    # 수작업 노이즈
                    elif df.loc[i]['AOSTC_RFNG_MTHD_GB'] != 'M' and df.loc[i]['QLTY_ANLS_RSLT_CODE'] != '00':
                        self.manual_noise += 1
                
                        

                # 필터링 엔진 기타 ( 원본 4초 미만, 기타에러)
                elif df.loc[i]['FILTRG_ERR_DETL_CODE'] == '10' or df.loc[i]['FILTRG_ERR_DETL_CODE'] == '90':
                    self.filter_etc_err += 1
                    
                    # 수작업 레포트 O
                    if df.loc[i]['AOSTC_RFNG_MTHD_GB'] == 'M' and df.loc[i]['AI_CPBLTCU_ISU_YN'] == 'Y' and df.loc[i]['QLTY_ANLS_RSLT_CODE'] == '00':
                        self.manual_re_success += 1

                    # 수작업 레포트 X
                    elif df.loc[i]['AOSTC_RFNG_MTHD_GB'] == 'M' and df.loc[i]['AI_CPBLTCU_ISU_YN'] == 'N' and df.loc[i]['QLTY_ANLS_RSLT_CODE'] == '00':
                        self.manual_re_fail += 1
                    
                    # 수작업 기타에러
                    else:
                        self.manual_etc_err += 1
                
                
                else:
                    if df.loc[i]['AOSTC_RFNG_MTHD_GB'] == 'M' and (df.loc[i]['QLTY_ANLS_STS_CODE'] == '10' or df.loc[i]['QLTY_ANLS_STS_CODE'] == '00') and df.loc[i]['AI_CPBLTCU_ISU_YN'] == 'Y' and self.anls_dttm <= '2022-05-10':
                        self.manual_re_success += 1
                        # a.append(df.loc[i])
                    elif df.loc[i]['AOSTC_RFNG_MTHD_GB'] == 'M' and (df.loc[i]['QLTY_ANLS_STS_CODE'] == '10' or df.loc[i]['QLTY_ANLS_STS_CODE'] == '00') and df.loc[i]['AI_CPBLTCU_ISU_YN'] == 'N' and self.anls_dttm <= '2022-05-10':
                        self.manual_re_fail += 1
                    elif (df.loc[i]['FILTRG_ERR_DETL_CODE'] is None or df.loc[i]['FILTRG_ERR_DETL_CODE'] == '00') and df.loc[i]['AOSTC_RFNG_CLN_YN'] == 'N' and df.loc[i]['AI_CPBLTCU_ISU_YN'] == 'N':
                        self.filter_re_fail += 1
                    else:
                        self.filter_etc_err += 1
                        self.manual_etc_err += 1
                        
                        
                    

            if (i != len(df)-1 and self.anls_dttm == str(df.loc[i + 1]['ANLS_DTTM']).split(' ')[0]):
                # 현재 반복차수가 마지막 행이 아니면서
                # 다음 행과 비교하여 분석일자가 같을 경우
                pass
            else:
                # 다른
                # row 추가: anls_dttm 값이 다를 경우 기존 값 list에 append
                
                fail_total = self.filter_re_fail + self.manual_re_fail
                noise_etc_total = self.manual_noise + self.manual_etc_err

                
                self.total_list.append(self.total)
                self.no_wav_list.append(self.no_wav)
                self.anls_dttm_list.append(self.anls_dttm)
                
                self.filter_re_success_list.append(self.filter_re_success)
                self.filter_re_fail_list.append(fail_total)
                self.filter_noise_list.append(self.filter_noise)
                self.filter_etc_err_list.append(self.filter_etc_err)
                self.noise_etc_total_list.append(noise_etc_total)
                
                self.manual_re_success_list.append(self.manual_re_success)
                self.manual_re_fail_list.append(self.manual_re_fail)
                self.manual_noise_list.append(self.manual_noise)
                self.manual_etc_err_list.append(self.manual_etc_err)


                # 변수 초기화
                self.total = self.no_wav = self.filter_re_success = self.filter_re_fail = self.filter_noise = self.filter_etc_err = self.manual_re_success = self.manual_re_fail = self.manual_noise = self.manual_etc_err = fail_total = noise_etc_total = 0
    
    def report_detl(self, begin, end):
        
        if self.env == '개발':
            conn = pymysql.connect(host="DB접속 주소",
                        port=13306, user='사용자',
                        passwd='비밀번호',
                        db='DB',
                        charset='utf8')
        else:
            conn = pymysql.connect(host="DB접속 주소",
                        port=13306, user='사용자',
                        passwd='비밀번호',
                        db='DB',
                        charset='utf8')
        cursor = conn.cursor()
        conn.ping(reconnect=True)
        query = f"SELECT DATE_FORMAT(dta.ANLS_DTTM,'%Y-%m-%d'), \
                    dta.CLNTCO_REQ_MNGT_NO, \
                    rpt.CAR_REG_NO, \
                    (SELECT prdt.AFCM_CAR_ALL_NM FROM TAB_L_CAR_PRDT_LIST prdt \
                        WHERE dta.CAR_PRDT_MNGT_NO = prdt.CAR_PRDT_MNGT_NO) AS AFCM_CAR_ALL_NM, \
                    rpt.CAR_BRND_NM, \
                    rpt.CAR_CLAS_MDL_NM, \
                    rpt.CAR_MDL_NM, \
                    rpt.CAR_MDL_GRD_NM, \
                    rpt.MID_RANGE_SIGNAL_SUM, \
                    rpt.LOW_HIGH_RANGE_SIGNAL_SUM, \
                    rpt.PERIODIC_SIGNAL_SUM, \
                    rpt.APERIODIC_SIGNAL_SUM, \
                    rpt.HUMAN_FRIENDLY_SIGNAL_SUM, \
                    rpt.SUM_SCR \
                FROM TBD_L_CUST_REQ_ATP_DTA dta \
                INNER JOIN TBD_L_AI_CPBLT_CU_RPT rpt \
                ON dta.AI_CPBLTCU_ISU_NO = rpt.AI_CPBLTCU_ISU_NO \
                AND dta.CLNTCO_REQ_MNGT_NO = rpt.CLNTCO_REQ_MNGT_NO \
                WHERE (((dta.INIT_FILTRG_PRCS_GB = '~' OR dta.INIT_FILTRG_PRCS_GB = '00') \
                    AND dta.QLTY_ANLS_STS_CODE ='00' \
                    AND dta.AOSTC_RFNG_MTHD_GB != 'M') \
                OR (((dta.FILTRG_ERR_DETL_CODE = '20' OR dta.FILTRG_ERR_DETL_CODE = '21' OR dta.FILTRG_ERR_DETL_CODE = '22') \
                    OR (dta.FILTRG_ERR_DETL_CODE = '10' OR dta.FILTRG_ERR_DETL_CODE = '90')) \
                    AND (dta.AOSTC_RFNG_MTHD_GB = 'M' AND dta.AI_CPBLTCU_ISU_YN = 'Y' AND dta.QLTY_ANLS_RSLT_CODE = '00')) \
                OR (dta.AOSTC_RFNG_MTHD_GB = 'M' AND (dta.QLTY_ANLS_STS_CODE = '10' OR dta.QLTY_ANLS_STS_CODE = '00') \
                    AND dta.AI_CPBLTCU_ISU_YN = 'Y' \
                    AND DATE_FORMAT(dta.ANLS_DTTM, '%Y-%m-%d') <= '2022-05-10')) \
                AND DATE_FORMAT(dta.ANLS_DTTM, '%Y-%m-%d') >= DATE_FORMAT('{begin}', '%Y-%m-%d') \
                AND DATE_FORMAT(dta.ANLS_DTTM, '%Y-%m-%d') <= DATE_FORMAT('{end}', '%Y-%m-%d') \
                ORDER BY dta.ANLS_DTTM"
        cursor.execute(query)
        row = cursor.fetchall()
        conn.close()
        data_detl = pd.DataFrame(row, columns=self.col)
        return data_detl
    
    def make_tally_df(self, report_df):


        data_total_tally = pd.DataFrame({'분석일시' : self.anls_dttm_list,
                                '총 요청' : self.total_list,
                                '고객 최종 차량 정보 여부 "N"' : self.filter_re_fail_list,
                                '원본없음' : self.no_wav_list,
                                '레포트 자동화 생성' : self.filter_re_success_list,
                                '레포트 수기 생성' : self.manual_re_success_list,
                                '레포트 생성 실패(노이즈, 기타에러)' : self.noise_etc_total_list,
                                '필터 노이즈' : self.filter_noise_list,
                                '필터 기타' : self.filter_etc_err_list,
                                '수동 노이즈' : self.manual_noise_list,
                                '수동 기타' : self.manual_etc_err_list})


        file_name = f'{self.now_date}_{self.env}_집계내역_보고서생성여부{str(self.start_date)}_{str(self.end_date)}.xlsx'
        writer = pd.ExcelWriter(self.output, engine='openpyxl')

        data_total_tally.to_excel(writer, sheet_name='총 집계')
        report_df.to_excel(writer, sheet_name='상세 개별 내역')
        writer.save()
        processed_data = self.output.getvalue()
        
        return file_name, processed_data, data_total_tally

if __name__ == '__main__':
    
    main = reportsuccess()