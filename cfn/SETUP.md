## 方法1: AWS CLI から実行する手順

### 前提準備: デフォルトVPC IDの確認
ターミナルで以下のコマンドを実行してデフォルトVPC IDを取得します。
```bash
DEFAULT_VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query "Vpcs[0].VpcId" --output text)
echo "Default VPC ID: ${DEFAULT_VPC_ID}"
```

### ステップ1: 共有ネットワークのデプロイ
```bash
# スタックの作成
aws cloudformation deploy \
  --template-file cfn/network-base.yml \
  --stack-name study-mwaa-network \
  --parameter-overrides ExistingVpcId=${DEFAULT_VPC_ID}

# 作成されたサブネットIDとセキュリティグループIDを変数に取得
SUBNET1_ID=$(aws cloudformation describe-stacks --stack-name study-mwaa-network --query "Stacks[0].Outputs[?OutputKey=='PrivateSubnet1Id'].OutputValue" --output text)
SUBNET2_ID=$(aws cloudformation describe-stacks --stack-name study-mwaa-network --query "Stacks[0].Outputs[?OutputKey=='PrivateSubnet2Id'].OutputValue" --output text)
SG_ID=$(aws cloudformation describe-stacks --stack-name study-mwaa-network --query "Stacks[0].Outputs[?OutputKey=='SecurityGroupId'].OutputValue" --output text)

echo "Subnet 1: ${SUBNET1_ID}"
echo "Subnet 2: ${SUBNET2_ID}"
echo "Security Group: ${SG_ID}"
```

### ステップ2: MWAA環境のデプロイ
> [!IMPORTANT]
> MWAA用のIAMロールを作成するため、`--capabilities CAPABILITY_IAM` が必須です。

```bash
aws cloudformation deploy \
  --template-file cfn/mwaa-environment.yml \
  --stack-name study-mwaa-env \
  --parameter-overrides \
      PrivateSubnet1Id=${SUBNET1_ID} \
      PrivateSubnet2Id=${SUBNET2_ID} \
      SecurityGroupId=${SG_ID} \
      EnvironmentName=study-mwaa \
  --capabilities CAPABILITY_IAM
```
※作成完了までに約20〜30分かかります。完了後、以下のコマンドでWebserver URLを確認できます。
```bash
aws cloudformation describe-stacks \
  --stack-name study-mwaa-env \
  --query "Stacks[0].Outputs[?OutputKey=='MwaaApacheAirflowUI'].OutputValue" \
  --output text
```

### ステップ3: 課金停止（削除）コマンド
```bash
# 1. S3バケット名を取得して中身を全削除
BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name study-mwaa-env --query "Stacks[0].Outputs[?OutputKey=='EnvironmentBucketArn'].OutputValue" --output text | awk -F::: '{print $2}')
aws s3 rm "s3://${BUCKET_NAME}" --recursive

# 2. MWAAスタックを削除
aws cloudformation delete-stack --stack-name study-mwaa-env
aws cloudformation wait stack-delete-complete --stack-name study-mwaa-env

# 3. ネットワークスタックを削除
aws cloudformation delete-stack --stack-name study-mwaa-network
aws cloudformation wait stack-delete-complete --stack-name study-mwaa-network
```

---

## 方法2: AWS マネジメントコンソール（GUI）から実行する手順

### ステップ1: 共有ネットワークの作成
1. AWSマネジメントコンソールで CloudFormation を開き、「スタックの作成」>「新しいリソースを使用 (標準)」をクリック。
2. 「テンプレートの指定」で「テンプレートファイルのアップロード」を選択し、`cfn/network-base.yml` を選択して「次へ」。
3. スタック名に `study-mwaa-network` を入力。
4. パラメータ `ExistingVpcId` に、ご自身のデフォルトVPC ID（`vpc-xxxxxx`）を入力して「次へ」。
5. 「スタックオプションの設定」はデフォルトのまま「次へ」進み、「スタックの作成」をクリック。
6. ステータスが `CREATE_COMPLETE`（約3〜5分）になったら、「出力」タブを開き、以下の値をメモします：
   - `PrivateSubnet1Id`
   - `PrivateSubnet2Id`
   - `SecurityGroupId`

### ステップ2: MWAA環境の作成
1. 再度 CloudFormation コンソールで「スタックの作成」>「新しいリソースを使用 (標準)」をクリック。
2. `cfn/mwaa-environment.yml` をアップロードして「次へ」。
3. スタック名に `study-mwaa-env` を入力。
4. パラメータに、ステップ1でメモした以下の値を入力：
   - `PrivateSubnet1Id`: （AZ1のサブネットID）
   - `PrivateSubnet2Id`: （AZ2のサブネットID）
   - `SecurityGroupId`: （セキュリティグループID）
   - `EnvironmentName`: `study-mwaa`（任意）
   - （その他のログ設定やWorker数はデフォルトのままでOK）
5. 「次へ」>「次へ」と進み、最後の確認画面の最下部にある 「AWS CloudFormation によって IAM リソースがカスタム名で作成される場合があることを承認します」にチェック を入れ、「スタックの作成」をクリック。
6. ステータスが `CREATE_COMPLETE`（約20〜30分）になったら、「出力」タブにある `MwaaApacheAirflowUI` のURLをクリックしてAirflow画面にログインします。

### ステップ3: 課金停止（削除）手順
1. S3バケットを空にする:
   - AWSコンソールの S3 を開く。
   - `studymwaa-xxxx-environmentbucket-xxxx` バケットを選択し、「空にする」をクリックして実行。
2. MWAAスタックの削除:
   - CloudFormationコンソールで `study-mwaa-env` を選択し、「削除」をクリック（約15〜20分）。
3. ネットワークスタックの削除:
   - `study-mwaa-env` の削除完了後、`study-mwaa-network` を選択して「削除」をクリック（約3〜5分）。

---

どちらの方法でも同様に安全に構築・削除できますので、やりやすい方でお試しください。



## 作成確認
[./mwaa_setup_check.png]