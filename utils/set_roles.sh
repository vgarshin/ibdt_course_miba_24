#!/usr/bin/env bash

# read list of federated users from file
# and setting or removing roles for folder

# flag "add" or "remove"
FLAG="add"

# flag for organization roles
ORGFLAG="set" # ibdmt course
#ORGFLAG="no" # e2e course
ORGID="bpfldhk2bssh1eebr06j"

# flag to "create" folders
FOLDER="create"

# flag to "create" service accounts
SRVACC="create" # ibdmt course
#SRVACC="no" # e2e course

# main cloud
CLOUD="cloud-gsom"

# name of common folder
COMMONFOLDER="common"

# iterate over users
while read lineuser; do
  echo ============================================================
  userarr=($lineuser)

  # get user id
  output=( $(yc organization-manager user list --organization-id bpfldhk2bssh1eebr06j | grep ${userarr[2]}) )

  # list user's info
  echo user name ${userarr[0]}
  echo user surname ${userarr[1]}
  echo user email ${userarr[2]}
  foldername="${userarr[2]%@*}"
  foldername=${foldername//./""}
  echo user folder ${foldername}
  echo user id ${output[1]}

  # base role for a cloud
  echo setting role resource-manager.clouds.member for cloud ${CLOUD}
  yc resource-manager cloud add-access-binding ${CLOUD} \
    --role resource-manager.clouds.member \
    --subject userAccount:${output[1]}

  # create folder if necessary
  if [ ${FOLDER} = "create" ]
  then
    echo folder to create ${foldername}
    descr="${userarr[0]} ${userarr[1]}"
    yc resource-manager folder create \
      --name ${foldername} \
      --description "${descr}"
  fi

  # set roles for organization if necessary
  if [ ${ORGFLAG} = "set" ]
  then
    while read linerole; do
      rolearr=($linerole)
      role=${linerole[0]}
      echo setting role ${role} for oranization id ${ORGID}
      yc organization-manager organization add-access-binding ${ORGID} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    done < roles_org.txt
  fi

  # create service account if necessary
  # and set roles for this account
  if [ ${SRVACC} = "create" ]
  then
    # create service account
    srvaccname="srv-acc-${userarr[2]%@*}"
    echo creating service account ${srvaccname} in folder ${userarr[2]%@*}
    descr="${userarr[0]} ${userarr[1]}"
    yc iam service-account create \
      --name ${srvaccname} \
      --folder-name ${foldername} \
      --description "Service account for ${descr}"
    # identify id of service account
    output_srv=( $( yc iam service-account list --folder-name ${foldername} | grep ${srvaccname}) )
    # set roles for service account
    while read linerole; do
      rolearr=($linerole)
      role=${linerole[0]}
      echo setting role ${role} for ${srvaccname}
      yc resource-manager folder add-access-binding ${foldername} \
        --role ${role} \
        --subject serviceAccount:${output_srv[1]}
    done < roles_srv_ibdmt.txt
  fi

  echo current roles in ${foldername}
  yc resource-manager folder list-access-bindings ${foldername}

  # iterate over roles and add or remove roles
  # for folder to federated user
  while read linerole; do
    rolearr=($linerole)
    role=${linerole[0]}
    if [ ${FLAG} = "add" ]
    then
      # adding role
      echo setting role ${role}
      yc resource-manager folder add-access-binding ${foldername} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    elif [ ${FLAG} = "remove" ]
    then
      # removing role
      echo removing role ${role}
      yc resource-manager folder remove-access-binding ${foldername} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    else
      echo "no action"
    fi
  done < roles_ibdmt.txt # ibdmt course
  #done < roles_e2e.txt # e2e course

  # iterate over common roles and add or remove
  # roles for common folder to federated user
  while read linerole; do
    rolearr=($linerole)
    role=${linerole[0]}
    if [ ${FLAG} = "add" ]
    then
      # adding role
      echo setting common role ${role} in folder ${COMMONFOLDER}
      yc resource-manager folder add-access-binding ${COMMONFOLDER} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    elif [ ${FLAG} = "remove" ]
    then
      # removing role
      echo removing common role ${role} in folder ${COMMONFOLDER}
      yc resource-manager folder remove-access-binding ${COMMONFOLDER} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    else
      echo "no action"
    fi
  done < roles_common.txt

  echo new roles in ${foldername}
  yc resource-manager folder list-access-bindings ${foldername}

done < students_ibdmt_24.txt # ibdmt course
#done < students_e2e_24.txt # e2e course