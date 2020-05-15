pipeline {
    agent {
        node {
            label 'java'
        }
    }
    stages {
        stage ('Initialize') {
            steps {
                sh '''
                    echo "PATH = ${PATH}"
                    echo "M2_HOME = ${M2_HOME}"
                    cat "${M2_HOME}/settings.xml"
                '''
            }
        }

        stage ('Build') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'internal-repository', passwordVariable: 'PASSWORD_VAR', usernameVariable: 'USERNAME_VAR')])
                {
                    sh 'mvn clean deploy -P ci-internal -Dnexus-nwse.username=${USERNAME_VAR} -Dnexus-nwse.password=${PASSWORD_VAR}'
                }
            }
        }
    }
    post {
        success {
            junit 'target/surefire-reports/**/*.xml'
        }
    }
}
