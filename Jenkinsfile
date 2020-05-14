pipeline {
    agent {
        node {
            label 'java'
        }
    }
    tools {
        maven 'Maven 3.3.9'
        jdk 'jdk8'
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
            withCredentials([usernamePassword(credentialsId: 'internal-repository', passwordVariable: 'PASSWORD_VAR', usernameVariable: 'USERNAME_VAR')])
            {
                sh 'mvn clean deploy -Dserver.username=${USERNAME_VAR} -Dserver.password=${PASSWORD_VAR}'
            }
            post {
                success {
                    junit 'target/surefire-reports/**/*.xml'
                }
            }
        }
    }
}
