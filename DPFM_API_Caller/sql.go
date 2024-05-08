package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-post-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-post-deletes-rmq-kube/DPFM_API_Output_Formatter"

	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) HeaderRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.Post = %d ", input.Header.Post)
	rows, err := c.db.Query(
		`SELECT 
			header.Post
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_post_header_data as header ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) FriendsRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Friend {
	where := fmt.Sprintf("WHERE friend.Post IS NOT NULL\nAND header.Post = %d", input.Header.Post)
	rows, err := c.db.Query(
		`SELECT 
			friend.Post, friend.Friend
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_post_friend_data as friend
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_post_header_data as header
		ON header.Post = friend.Post ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToFriend(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
