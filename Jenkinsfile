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
                    which mvn
                '''
            }
        }

        stage ('Build') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'nexus-sysarch-deploy', passwordVariable: 'PASSWORD_VAR', usernameVariable: 'USERNAME_VAR')])
                {
                    sh 'mvn clean test -s settings.xml -P ci-internal -Dserver.username=${USERNAME_VAR} -Dserver.password=${PASSWORD_VAR}'
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
