pipeline {
    agent any

    environment {
        DOCKER_IMAGE = "enterprise-registry.local/trade-migration"
        DOCKER_TAG = "${env.BUILD_NUMBER}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Linting') {
            steps {
                echo 'Running flake8 static code analysis...'
                sh '''
                    pip install flake8
                    flake8 dags/ mock_legacy_data.py --max-line-length=120
                '''
            }
        }

        stage('Docker Build') {
            steps {
                echo 'Building Airflow custom image...'
                sh '''
                    cat <<EOF > Dockerfile
FROM apache/airflow:2.7.2
COPY dags/ /opt/airflow/dags/
COPY sql/ /opt/airflow/sql/
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
EOF
                    docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} .
                '''
            }
        }

        stage('Docker Push') {
            steps {
                echo 'Pushing artifact to enterprise registry...'
                sh 'docker push ${DOCKER_IMAGE}:${DOCKER_TAG}'
            }
        }
    }

    post {
        always {
            cleanWs()
        }
        failure {
            echo 'Pipeline failed. Alerting engineering team.'
        }
    }
}
