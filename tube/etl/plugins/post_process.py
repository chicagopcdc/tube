import yaml

from tube.settings import PROJECT_TO_RESOURCE_PATH


def add_auth_resource_path(df):
    # add 'auth_resource_path' to resulting es document if 'project_id' exist
    if "project_id" in df[1]:
        project_id = df[1]["project_id"]

        # TODO check case with list?? make sure it is from the correct node - maybe add a node_type field? check during ETL?
        subject_id = ""
        if "person_submitter_id" in df[1]:
            subject_id += "/persons/{}".format(df[1]["person_submitter_id"])
            if "submitter_id" in df[1]:
                subject_id += "/subjects/{}".format(df[1]["submitter_id"]) 

        if project_id is not None:
            if isinstance(project_id, list):
                df[1]["auth_resource_path"] = [
                    create_single_auth_resource_path(p, subject_id) for p in project_id
                ]
            else:
                df[1]["auth_resource_path"] = create_single_auth_resource_path(
                    project_id,
                    subject_id
                )
        else:
            df[1]["auth_resource_path"] = ""

        print("ABSOLUTE LUCA")
        # test commit for quay.io trigger #4
        print(df[1]["auth_resource_path"])

    return df[0], df[1]


def create_single_auth_resource_path(project_id, granular_path):
    s = project_id.split("-", 1)
    print(s)
    print(granular_path)
    print(PROJECT_TO_RESOURCE_PATH.get(s[1]))
    resource_path = PROJECT_TO_RESOURCE_PATH.get(s[1])
    if resource_path is None:
        auth_path = "/programs/{}/projects/{}".format(s[0], s[1])
        if granular_path is not None:
            auth_path += granular_path
        return auth_path
    if granular_path is not None:
        resource_path += granular_path
    return resource_path
