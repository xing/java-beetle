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
                    mvn --version
                    java -version
                    javac
                '''
            }
        }

        stage ('Build') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'nexus-sysarch-deploy', passwordVariable: 'PASSWORD_VAR', usernameVariable: 'USERNAME_VAR')])
                {
                    sh 'mvn help:effective-pom clean test -s settings.xml -P ci-internal -Dserver.username=${USERNAME_VAR} -Dserver.password=${PASSWORD_VAR} -q'
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
