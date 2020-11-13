/*
This sample application is part of the Timestream prerelease documentation. The prerelease documentation is confidential and is provided
under the terms of your nondisclosure agreement with Amazon Web Services (AWS) or other agreement governing your receipt of AWS confidential
information.
*/

// Constants
const DATABASE_NAME = 'rhythm_cloud';
const TABLE_NAME = 'rhythm-cloud-hits';
const HIT_TYPES = {
    USER: 'user',
    SYSTEM: 'system'
}

const GET_SCORE_QUERY_TEMPLATE =    "WITH system AS( " +
                                    "  SELECT time, drum, ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY time ASC) as sequence_id FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
                                    "  WHERE session_id = 'c15f9236-c945-4139-91a9-7c952ecb67b3' AND hit_type = '" + HIT_TYPES.SYSTEM + "' AND measure_name='drum' " + 
                                    "), " +
                                    "user AS( " + 
                                    "  SELECT time, drum, ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY time ASC) as sequence_id FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
                                    "  WHERE session_id = 'c15f9236-c945-4139-91a9-7c952ecb67b3' AND hit_type = '" + HIT_TYPES.USER + "' AND measure_name='drum' " +
                                    "), " +
                                    "joined AS( " + 
                                    "SELECT system.time AS a, user.time AS b, system.drum AS x, user.drum AS y, if(system.drum = user.drum, 1, 0) AS score FROM system INNER JOIN user ON(system.sequence_id = user.sequence_id) "
                                    ") " +
                                    "SELECT SUM(score) AS score FROM joined";

module.exports = {DATABASE_NAME, TABLE_NAME, HIT_TYPES};