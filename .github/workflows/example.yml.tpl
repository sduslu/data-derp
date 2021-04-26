name: 'example1-example2'

env:
  PROJECT_NAME: twdu-europe
  MODULE_NAME: example1-example2
  TF_WORKSPACE: example1-example2

on:
  push:
    branches:
    - example1-example2
    paths-ignore:
      - 'Dockerfile'
      - '.github/workflow/build-push-dev-container.yml'

jobs:
  base:
    name: 'Base Terraform'
    runs-on: self-hosted
    environment: production
    env:
      SUBDIR: base
    container:
      image: docker.pkg.github.com/kelseymok/terraform-workspace/terraform-workspace:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    steps:
    - name: Checkout
      uses: actions/checkout@v2

    - name: Assume Role
      run: assume-role twdu-europe-github-runner-aws

    - name: Terraform Setup Workspace
      uses: ./.github/composite-actions/terraform-setup-workspace

    - name: Terraform Init & Apply
      uses: ./.github/composite-actions/terraform-apply

  data-ingestion:
    name: 'Data Ingestion'
    runs-on: self-hosted
    environment: production
    env:
      SUBDIR: data-ingestion
    needs: ["base"]
    container:
      image: docker.pkg.github.com/kelseymok/terraform-workspace/terraform-workspace:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    steps:
      - name: Checkout
        uses: actions/checkout@v2

      - name: Assume Role
        run: assume-role twdu-europe-github-runner-aws

      - name: Upload Main.py
        run: |
          cd ${SUBDIR}/src
          aws s3 cp main.py s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/main.py

      - name: Upload Data Ingestion lib
        run: |
          cd ${SUBDIR}/src
          python setup.py bdist_egg
          filename=$(ls dist)
          aws s3 cp dist/${filename} s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/data_ingestion-0.1-py3.egg

      - name: Terraform Setup Workspace
        uses: ./.github/composite-actions/terraform-setup-workspace

      - name: Terraform Init & Apply
        uses: ./.github/composite-actions/terraform-apply

  data-transformation:
    name: 'Data Transformation'
    runs-on: self-hosted
    environment: production
    env:
      SUBDIR: data-transformation
    needs: [ "base" ]
    container:
      image: docker.pkg.github.com/kelseymok/terraform-workspace/terraform-workspace:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    steps:
      - name: Checkout
        uses: actions/checkout@v2

      - name: Assume Role
        run: assume-role twdu-europe-github-runner-aws

      - name: Upload Main.py
        run: |
          cd ${SUBDIR}/lab
          aws s3 cp main.py s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/main.py

      - name: Upload Data Transformation lib
        run: |
          cd ${SUBDIR}/lab
          python setup.py bdist_egg
          filename=$(ls dist)
          aws s3 cp dist/${filename} s3://${PROJECT_NAME}-${MODULE_NAME}/${SUBDIR}/data_transformation-0.1-py3.egg

      - name: Terraform Setup Workspace
        uses: ./.github/composite-actions/terraform-setup-workspace

      - name: Terraform Init & Apply
        uses: ./.github/composite-actions/terraform-apply

  data-workflow:
    name: 'Data Workflow'
    runs-on: self-hosted
    environment: production
    needs: ["data-ingestion", "data-transformation"]
    env:
      SUBDIR: data-workflow
    container:
      image: docker.pkg.github.com/kelseymok/terraform-workspace/terraform-workspace:latest
      credentials:
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    steps:
      - name: Checkout
        uses: actions/checkout@v2

      - name: Assume Role
        run: assume-role twdu-europe-github-runner-aws

      - name: Terraform Setup Workspace
        uses: ./.github/composite-actions/terraform-setup-workspace

      - name: Terraform Init & Apply
        uses: ./.github/composite-actions/terraform-apply

#  destroy:
#    name: 'Destroy all'
#    runs-on: self-hosted
#    environment: production
#    container:
#      image: docker.pkg.github.com/kelseymok/terraform-workspace/terraform-workspace:latest
#      credentials:
#        username: ${{ github.actor }}
#        password: ${{ secrets.GITHUB_TOKEN }}
#
#    steps:
#      - name: Checkout
#        uses: actions/checkout@v2
#
#      - name: Assume Role
#        run: assume-role twdu-europe-github-runner-aws
#
#      - name: Terraform Init & Destroy (Data Workflow)
#        env:
#          SUBDIR: data-workflow
#        uses: ./.github/composite-actions/terraform-destroy
#
#      - name: Terraform Init & Destroy (Data Transformation)
#        env:
#          SUBDIR: data-transformation
#        uses: ./.github/composite-actions/terraform-destroy
#
#      - name: Terraform Init & Destroy (Data Ingestion)
#        env:
#          SUBDIR: data-ingestion
#        uses: ./.github/composite-actions/terraform-destroy

#      - name: Terraform Init & Destroy (Base)
#        env:
#          SUBDIR: base
#        uses: ./.github/composite-actions/terraform-destroy