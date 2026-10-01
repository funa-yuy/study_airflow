## MWAAの課金に関する重要な前提

MWAAには EC2 のような「一時停止（Stop/Pause）」機能がありません。
起動している間は、以下の固定料金が時間単位で発生し続けます：
- MWAA環境料金: 約 $0.49/時間（mw1.small・東京リージョンの場合、月換算で約 $350〜）
- VPCエンドポイント料金: 4つのInterfaceエンドポイント × 2AZ分 ＝ 約 $0.11/時間（月換算で約 $80〜）

したがって、「勉強が終わったらお金がかからない状態にする」ためには、CloudFormationスタックを削除（Delete Stack）する必要があります。

---

## 一連の流れ（作成 → 動作確認 → 課金停止）

### ステップ1: 作成（デプロイ）

#### 1. 共有ネットワークの作成（所要時間: 約3〜5分）
AWSマネジメントコンソールの CloudFormation または AWS CLI で [cfn/network-base.yml](file:///Users/miyuu/Desktop/999_programming/study_airflow/cfn/network-base.yml) をデプロイします。
- スタック名: `study-mwaa-network`（例）
- パラメータ `ExistingVpcId`: お使いのデフォルトVPC ID（例: `vpc-0123456789abcdef0`）
- 完了後、スタックの「出力（Outputs）」タブから `PrivateSubnet1Id`, `PrivateSubnet2Id`, `SecurityGroupId` をコピーします。

#### 2. MWAA環境の作成（所要時間: 約20〜30分）
[cfn/mwaa-environment.yml](file:///Users/miyuu/Desktop/999_programming/study_airflow/cfn/mwaa-environment.yml) をデプロイします。
- スタック名: `study-mwaa-env`（例）
- パラメータ:
  - `PrivateSubnet1Id`: 手順1で取得したID
  - `PrivateSubnet2Id`: 手順1で取得したID
  - `SecurityGroupId`: 手順1で取得したID
- 完了後、「出力（Outputs）」タブに表示される `MwaaApacheAirflowUI` のURLから Airflow Web UI にアクセスして動作確認を行います。

---

### ステップ2: 動作確認後のストップ（完全削除）手順

作成した時と逆の順番でスタックを削除します。

#### 1. S3バケットの中身を空にする（事前準備）
MWAA実行中にDAGファイルやログファイルがS3バケットに書き込まれます。バケット内にファイルが残っているとCloudFormationがバケット削除時にエラーになるため、先にバケット内を空にします。
- AWSマネジメントコンソールの S3 を開く。
- スタックで作成されたバケット（`studymwaa-xxxx-environmentbucket-xxxx`）を選択し、「空にする（Empty）」を実行。

#### 2. MWAA環境スタックの削除（所要時間: 約15〜20分）
- CloudFormation コンソールで `study-mwaa-env` スタックを選択し、「削除（Delete）」をクリック。
- これにより、MWAA本体とIAMロール、空のS3バケットが削除され、MWAA本体の課金が停止します。

#### 3. 共有ネットワークスタックの削除（所要時間: 約3〜5分）
- `study-mwaa-env` の削除が完了したら、`study-mwaa-network` スタックを選択して「削除（Delete）」をクリック。
- これにより、VPCエンドポイントやプライベートサブネットが削除され、エンドポイントの課金も完全にゼロになります。

---

## 学習時の運用に関する選択肢

次回以降、再度勉強する際のコストと手間のバランスについて2つの選択肢があります。

### 候補1: 毎回ネットワークも含めて完全削除（推奨：コスト最安）
- 概要: 勉強が終わったら `study-mwaa-env` と `study-mwaa-network` の両方を削除する。
- メリット: 勉強していない期間のコストが完全に0円になる。
- デメリット: 再開時にネットワークデプロイ（約3分）＋MWAAデプロイ（約25分）の待ち時間が発生する。
