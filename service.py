from typing import List, Dict

import aiomysql
from grpc.aio import ServicerContext
from grpc import StatusCode

from micro_services_protobuf.course_score_query import service_pb2_grpc as csq_grpc
from micro_services_protobuf.course_score_query import model_pb2 as csq_model
from micro_services_protobuf.mycqu_service import mycqu_model_pb2 as mycqu_model

from _321CQU.sql_helper.SqlManager import DatabaseConfig

from utils.sqlManager import CSQSqlManager

__all__ = ['CourseScoreQuery']


class CourseScoreQuery(csq_grpc.CourseScoreQueryServicer):
    async def FindCourseByName(self, request: csq_model.FindCourseByNameRequest, context: ServicerContext):
        async with CSQSqlManager().cursor(DatabaseConfig.Score) as cursor:
            cursor: aiomysql.Cursor
            if request.course_name != '':
                await cursor.execute('select cid, cname from Course where cname like %s',
                                     (f'%{request.course_name}%',))
            elif request.teacher_name != '':
                await cursor.execute('select T2.tname, C.cid, cname from Course C '
                                     'join Teaching T on C.cid = T.cid join Teacher T2 on T2.tid = T.tid '
                                     'where tname like %s', (f'%{request.teacher_name}%',))
            else:
                await context.abort(StatusCode.INVALID_ARGUMENT, '课程名或教师名关键词为空')

            res = await cursor.fetchall()
            if request.course_name != '':
                result = map(lambda x: mycqu_model.Course(name=x[1], code=x[0]), res)
            else:
                result = map(lambda x: mycqu_model.Course(name=x[2], code=x[1], instructor=x[0]), res)
        return csq_model.FindCourseByNameResponse(courses=result)

    async def FetchLayeredScoreDetail(self, request: csq_model.FetchLayeredScoreDetailRequest,
                                      context: ServicerContext):
        async with CSQSqlManager().cursor(DatabaseConfig.Score) as cursor:
            cursor: aiomysql.Cursor
            await cursor.execute('select distinct term from Score where cid = %s', (request.course_code,))
            res1 = await cursor.fetchall()
            terms = map(lambda x: x[0], res1)
            result: Dict[str, List[csq_model.LayeredScoreDetail.LayeredTermScoreDetail]] = {}
            for term in terms:
                await cursor.execute('call LayeredCourseDetail(%s, %s)', (request.course_code, term))
                res2 = await cursor.fetchall()
                details = map(
                    lambda x: {
                        'tname': x[0],
                        'detail': csq_model.LayeredScoreDetail.LayeredTermScoreDetail(
                            term=mycqu_model.CquSession(year=int(term[:4]), is_autumn=term[-1] == '秋'),
                            is_hierarchy=x[2], max=x[3], min=x[4], average=x[5], num=x[6],
                            level1_num=x[7], level2_num=x[8], level3_num=x[9], level4_num=x[10], level5_num=x[11]
                        )
                    }, res2)

                for detail in details:
                    if detail['tname'] in result.keys():
                        result[detail['tname']].append(detail['detail'])
                    else:
                        result[detail['tname']] = [detail['detail']]
        return csq_model.FetchLayeredScoreDetailResponse(
            course_name=res2[0][1], course_code=request.course_code,
            score_details=map(lambda item: csq_model.LayeredScoreDetail(teacher_name=item[0], details=item[1]),
                              result.items()))
