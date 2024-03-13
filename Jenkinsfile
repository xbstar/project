class Config {
    static Boolean EXEC=true;
}
pipeline {
    agent any
    environment {
        DEBUG = false
    }
    parameters {
        booleanParam(name: 'scloud', defaultValue: true, description: '是否构建并部署scloud?')
    }
    stages {
        stage('过程配置') {
            agent none
            steps {
                script {
                    def RES= input(submitterParameter: 'admin',
                            message: '配置代码拉取策略',
                            parameters: [
                                    booleanParam(name: 'EXEC', defaultValue: Config.EXEC, description: '是否重新拉取代码'),
                            ], ok: '配置')
                    Config.EXEC=RES.EXEC
                }
            }
        }
        stage('打印一下') {
            agent none
            when {
                environment name: 'scloud', value: 'true'
                expression {
                    Config.EXEC==true
                }

            }
            steps {
                echo "${scloud}"
            }
        }
    }
}
