#!/usr/bin/env python
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Demonstrates how to create a shared list of negative broad match keywords.

Note that the keywords will be attached to the specified campaign.
"""


import argparse
import sys
import uuid

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from time import sleep

import logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(name)s - %(levelname)s] %(message).5000s')
logging.getLogger('google.ads.googleads.client').setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)
# logger.disabled = True

def get_google_ads_id_from_ressource_name(ressource_name):
    """
    Google Ads API return the google ids in the response path, so in order
    to get the int value of this id, call this function

    :param ressource_name:  "customers/3543058835/campaignBudgets/10050260545"
                            "customers/6762619597/adGroupAds/131272453419~569688046440"
    :return:
    """
    output = None

    if ressource_name:
        # 10050260545 or 131272453419~569688046440
        path_end = ressource_name.split('/')[-1]
        if "~" in path_end:
            # We dont return the ad group id, since this information is usually available in the response
            ad_group_id, item_id = path_end.split("~")
            output = int(item_id)
        else:
            output = int(path_end)

    return output



def get_batch_job_results_when_ready(client, batch_job_resource_name, service, max_poll_attempts=1452):
    request = client.get_type("ListBatchJobResultsRequest")
    request.resource_name = batch_job_resource_name
    request.response_content_type = client.enums.ResponseContentTypeEnum.MUTABLE_RESOURCE
    request.page_size = 10000

    max_sleep_interval = 60  # Sleep for 1 min
    min_sleep_interval = 5  # Sleep for 5 secs
    poll_attempt = 0

    while (poll_attempt in range(max_poll_attempts)):
        try:
            poll_attempt += 1
            batch_job_results = service.list_batch_job_results(request=request)
            return batch_job_results
        except Exception as ex:
            tmp_sleep_interval = min_sleep_interval * poll_attempt
            sleep_interval = (min_sleep_interval
                              if tmp_sleep_interval < max_sleep_interval
                              else max_sleep_interval)
            logger.info(
                'Batch Job not ready, sleeping for %s seconds. Poll attempt: %d' %
                (sleep_interval, poll_attempt))
            sleep(sleep_interval)

    raise Exception('Batch Job not finished downloading. Try checking later.\n'
                    'Batch_job details {}\nPoll attempt : {}'.format(batch_job_resource_name, poll_attempt))


def get_mutate_operation(client, operation_type, operation):
    if not operation_type:
        msg = """ERROR 28-04: Attribute operation_type is not defined, this is required 
        to call assemble() function"""
        raise Exception(msg)

    mutate_operation = client.get_type("MutateOperation")
    client.copy_from(getattr(mutate_operation, operation_type), operation)

    return mutate_operation


def create_share_set(client, customer_id):
    service = client.get_service("BatchJobService")
    batch_job_operation = client.get_type('BatchJobOperation')
    batch_job = client.get_type('BatchJob')
    client.copy_from(batch_job_operation.create, batch_job)

    response = service.mutate_batch_job(
        customer_id=customer_id, operation=batch_job_operation
    )

    batch_job_resource_name = response.result.resource_name
    sequence_token = None

    shared_set_operation = client.get_type("SharedSetOperation")
    shared_set = shared_set_operation.create
    shared_set.name = f"Manager API Negative keyword list = batch - {uuid.uuid4()}"
    shared_set.type_ = client.enums.SharedSetTypeEnum.NEGATIVE_KEYWORDS

    mutate_operations = [get_mutate_operation(client, 'shared_set_operation', shared_set_operation)]

    service.add_batch_job_operations(resource_name=batch_job_resource_name, mutate_operations=mutate_operations,
                                     sequence_token=sequence_token, timeout=99999)
    service.run_batch_job(resource_name=batch_job_resource_name, timeout=999999)

    batch_result = get_batch_job_results_when_ready(client, batch_job_resource_name, service)
    mutate_operation_response = batch_result.results[0].mutate_operation_response
    shared_set_result = mutate_operation_response.shared_set_result
    return shared_set_result


def main(client, customer_id, campaign_id, manager_id):
    campaign_service = client.get_service("CampaignService")
    shared_set_service = client.get_service("SharedSetService")
    shared_criterion_service = client.get_service("SharedCriterionService")
    campaign_shared_set_service = client.get_service("CampaignSharedSetService")

    shared_set_result = create_share_set(client, manager_id)
    shared_set_resource_name = shared_set_result.resource_name
    shared_set_resource_id = get_google_ads_id_from_ressource_name(shared_set_resource_name)

    # Keywords to create a shared set of.
    keywords = ["mars cruise", "mars hotels"]
    shared_criteria_operations = []
    for keyword in keywords:
        shared_criterion_operation = client.get_type("SharedCriterionOperation")
        shared_criterion = shared_criterion_operation.create
        shared_criterion.keyword.text = keyword
        shared_criterion.keyword.match_type = (
            client.enums.KeywordMatchTypeEnum.BROAD
        )
        shared_criterion.shared_set = shared_set_resource_name
        shared_criteria_operations.append(shared_criterion_operation)
    try:
        response = shared_criterion_service.mutate_shared_criteria(
            customer_id=manager_id, operations=shared_criteria_operations
        )

        for shared_criterion in response.results:
            logger.info(
                "Created shared criterion "
                f'"{shared_criterion.resource_name}".'
            )
    except GoogleAdsException as ex:
        _handle_googleads_exception(ex)

    campaign_set_operation = client.get_type("CampaignSharedSetOperation")
    campaign_set = campaign_set_operation.create
    campaign_set.campaign = campaign_service.campaign_path(
        customer_id, campaign_id
    )
    campaign_set.shared_set = shared_set_service.shared_set_path(customer_id, shared_set_resource_id)

    logger.info(campaign_set_operation)

    try:
        campaign_shared_set_resource_name = (
            campaign_shared_set_service.mutate_campaign_shared_sets(
                customer_id=customer_id, operations=[campaign_set_operation]
            )
        )

        logger.info(
            "Created campaign shared set ",
            campaign_shared_set_resource_name.results[0].resource_name
        )
    except GoogleAdsException as ex:
        _handle_googleads_exception(ex)


def _handle_googleads_exception(exception):
    logger.info(
        f'Request with ID "{exception.request_id}" failed with status '
        f'"{exception.error.code().name}" and includes the following errors:'
    )
    for error in exception.failure.errors:
        logger.info(f'\tError with message "{error.message}".')
        if error.location:
            for field_path_element in error.location.field_path_elements:
                logger.info(f"\t\tOn field: {field_path_element.field_name}")
    sys.exit(1)


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(version="v9")

    parser = argparse.ArgumentParser(
        description=(
            "Adds a list of negative broad match keywords to the "
            "provided campaign, for the specified customer."
        )
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-c",
        "--customer_id",
        type=str,
        required=True,
        help="The Google Ads customer ID.",
    )
    parser.add_argument(
        "-i", "--campaign_id", type=str, required=True, help="The campaign ID."
    )
    parser.add_argument(
        "-m", "--manager_id", type=str, required=True, help="The campaign manager ID."
    )
    args = parser.parse_args()

    main(googleads_client, args.customer_id, args.campaign_id, args.manager_id)
