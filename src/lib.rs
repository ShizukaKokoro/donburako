//! Donburako
//!
//! データ駆動のワークフローを作成するためのライブラリ。
//! ワークフローは、ノードとエッジから構成される有向グラフで表現される。
//! ノードは、ワークフローのステップを表し、エッジはノード間の依存関係を表す。
//! 依存関係のないノードは並列に実行されるため、ワークフローの実行は効率的に行われる。
//!
//! # 概要
//!
//! ## イメージ
//!
//! 桃太郎が桃の中に入った状態で、川を流れているように、データをコンテナに入れて、ワークフローを流すイメージ。
//!
//! ## コンポーネント
//!
//! ## コンテナ
//!
//! データを格納するためのコンテナ。
//! 中にデータが入っており、エッジを通ってノードに渡される。
//! コンテナは、プロセッサーに入れられ、次に通るべきエッジをノードから指定されて、次のノードに渡される。
//! コンテナは、ノードごとに生成・破棄されるのではなく、基本的に複製され、データの統合の際と、ワークフローの終了時に破棄される。
//!
//! ### ノード
//!
//! 実行されるタスクを表す。
//! 非同期に実行されるため、 IO や通信などの、 CPU バウンドでない処理を効率的に並列実行できる。
//! CPU バウンドかどうかを判断でき、バウンドしている場合は、ブロッキングスレッドを生成して並列実行する。そうでない場合は、非同期タスクとして実行する。
//! 実行可能になると、ノードはキューに追加され、最大の並列実行数を超えないように実行される。
//! 全てのノードのハンドルを `select!` マクロで一定時間ごとにチェックすることで、ノードの実行状況を監視できる。
//!
//! ### ポート
//!
//! ノード間の依存関係を表す。
//! 入力ポートと出力ポートがある。
//! データ一つにつき、1組のポートが存在する。そのデータがどこから来るか、どこに行くかを表す。
//! 一つのノードが複数のポートを前にも後にも持つことができる。これにより、複数の引数と複数の戻り値を扱うことができる。
//! それぞれ始点と終点が同じかつ、データ型も同じようなポートを許容する。
//!
//!
//! ### ワークフロー
//!
//! ノードとポートの集合。
//! どの入力ポートがどのノードに接続されているかも含まれる。
//! ワークフローが実行される形になる。
//! ワークフローは同時に実行されうる。
//!
//! ### プロセッサー
//!
//! ワークフローを実行するためのエンジン。
//! ワークフローを登録し、任意のタイミングで実行する。
//! ワークフローに対して、コンテナを生成し、最初の入力ポートのプロセッサーマップにデータを格納する。
//! 入力ポートのマップにデータが格納されると、その入力ポートに接続されたノードが実行される。
//! ノードが実行されるとノードは出力ポートを参照して、入力ポートのプロセッサーマップにコンテナを格納する。
//! プロセッサーマップは、コンテナを格納し、コンテナの数を確認できるようにする。
//!
//! # 今後の展望
//!
//! ロギング、エラーハンドリング、リトライ、タイムアウトなど、ワークフローの実行に必要な機能を追加する。
//! ループと条件分岐をサポートする。

#![warn(missing_docs, rustdoc::missing_crate_level_docs, unused_results)]

pub mod container;
pub mod node;
pub mod workflow;
