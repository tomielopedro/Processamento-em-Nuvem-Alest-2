from dataclasses import dataclass, field
from typing import Optional, List
import matplotlib.pyplot as plt
import networkx as nx

@dataclass
class Task:
    """Representa uma tarefa da árvore de execução."""
    task_name: str
    task_time: int
    children: List["Task"] = field(default_factory=list)
    parent: Optional["Task"] = None



    def __repr__(self):
        return f"{self.task_name}({self.task_time})"


class TaskScheduler:
    """
    Classe para carregar árvore de tarefas, escalonar execução paralela,
    gerar relatório e desenhar a árvore.
    """
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.raiz = None
        self.num_proc = 1
        self.politica = "MAX"
        self._load_tree()

    @staticmethod
    def parse_task(raw: str) -> Task:
        """Converte string 'Nome_tempo' em objeto Task."""
        nome, tempo = raw.split("_")
        return Task(task_name=nome, task_time=int(tempo))

    def _load_tree(self):
        """Carrega a árvore de tarefas do arquivo e define raiz e processadores."""
        nodes = {}
        pais = set()
        proc = 1

        with open(self.file_path, "r") as f:
            for linha in f:
                linha = linha.strip()
                if "#" in linha:
                    proc = int(linha[7:].strip())
                elif "->" in linha:
                    pai_raw, filho_raw = map(str.strip, linha.split("->"))

                    if pai_raw not in nodes:
                        nodes[pai_raw] = self.parse_task(pai_raw)
                    if filho_raw not in nodes:
                        nodes[filho_raw] = self.parse_task(filho_raw)

                    pai = nodes[pai_raw]
                    filho = nodes[filho_raw]

                    pai.children.append(filho)
                    filho.parent = pai
                    pais.add(pai_raw)

        # Raiz: quem nunca foi filho
        todos_filhos = {f"{c.task_name}_{c.task_time}" for n in nodes.values() for c in n.children}
        raiz_nome = list(pais - todos_filhos)[0]
        self.raiz = nodes[raiz_nome]
        self.num_proc = proc

    def mostrar_arvore(self, task: Optional[Task] = None, nivel=0):
        """Imprime a árvore hierárquica de tarefas."""
        if task is None:
            task = self.raiz
        pai_nome = task.parent.task_name if task.parent else "None"
        print("  " * nivel + f"- {task.task_name} (tempo={task.task_time}, pai={pai_nome})")
        for filho in task.children:
            self.mostrar_arvore(filho, nivel + 1)

    def coletar_tarefas(self, task: Optional[Task] = None) -> List[Task]:
        """Coleta todas as tarefas em uma lista usando DFS."""
        if task is None:
            task = self.raiz
        tarefas = []

        def dfs(tarefa: Task):
            tarefas.append(tarefa)
            for filho in tarefa.children:
                dfs(filho)

        dfs(task)
        return tarefas

    def escalonar(self):
        """
        Escalonamento paralelo de tarefas respeitando dependências.
        Política pode ser 'MIN' ou 'MAX'.
        Retorna tempo total e ordem de execução.
        """
        todas_tarefas = {id(t): t for t in self.coletar_tarefas()}
        dependencias = {id(t): 0 for t in todas_tarefas.values()}

        # Contagem inicial de dependências (apenas 1 nível: pai -> filho)
        for t in todas_tarefas.values():
            for filho in t.children:
                dependencias[id(filho)] += 1

        prontos = [t for t in todas_tarefas.values() if dependencias[id(t)] == 0]
        executando = []
        ordem_execucao = []
        tempo_total = 0

        while prontos or executando:
            # Ordena tarefas prontas
            prontos.sort(key=lambda t: t.task_time, reverse=(self.politica.upper() == "MAX"))

            # Aloca tarefas para processadores livres
            while len(executando) < self.num_proc and prontos:
                tarefa = prontos.pop(0)
                executando.append({"tarefa": tarefa, "restante": tarefa.task_time})

            if not executando:
                break

            # Avança tempo pelo menor restante
            menor_tempo = min(t["restante"] for t in executando)
            tempo_total += menor_tempo
            for t in executando:
                t["restante"] -= menor_tempo

            # Processa tarefas concluídas
            concluidas = [t for t in executando if t["restante"] == 0]
            executando = [t for t in executando if t["restante"] > 0]

            for item in concluidas:
                tarefa = item["tarefa"]
                ordem_execucao.append(tarefa.task_name)
                for filho in tarefa.children:
                    dependencias[id(filho)] -= 1
                    if dependencias[id(filho)] == 0:
                        prontos.append(filho)

        return tempo_total, ordem_execucao


    def compara_politicas(self):

        self.politica = 'MIN'
        min_tempo, min_ordem_execucao = self.escalonar()
        self.politica = 'MAX'
        max_tempo, max_ordem_execucao = self.escalonar()

        tarefas = self.coletar_tarefas()
        qtd = len(tarefas)
        soma_tempos = sum(t.task_time for t in tarefas)
        media = soma_tempos / qtd if qtd else 0
        proc_tarefas = round(len(tarefas)/self.num_proc, 2)

        melhor_politica = (
            "IGUAIS" if min_tempo == max_tempo
            else "MAX" if min_tempo > max_tempo
            else "MIN"
        )

        info = {
            "file": self.file_path,
            "proc": self.num_proc,
            "quantidade_tarefas": qtd,
            "melhor_politica":melhor_politica,
            "politica_max":max_tempo,
            "politica_min":min_tempo,
            "soma_total_tempos": soma_tempos,
            "proporcao_proc_tarefas": proc_tarefas,
            "media_tempo_por_tarefa": round(media, 2),


        }

        return info

    def to_dict(self, tempo) -> dict:
        """
        Retorna as informações do relatório em um dicionário.
        """
        tarefas = self.coletar_tarefas()
        qtd = len(tarefas)
        soma_tempos = sum(t.task_time for t in tarefas)
        media = soma_tempos / qtd if qtd else 0
        proc_tarefas = round(len(tarefas)/self.num_proc, 2)

        relatorio_dict = {
            "file":self.file_path,
            "proc":self.num_proc,
            "politica":self.politica,
            "quantidade_tarefas": qtd,
            "soma_total_tempos": soma_tempos,
            "soma_tempos_escalonado":tempo,
            "proporcao_proc_tarefas":proc_tarefas,
            "media_tempo_por_tarefa": round(media, 2),
        }
        return relatorio_dict

    def desenhar_arvore(self):
        """Desenha a árvore usando networkx e matplotlib."""
        G = nx.DiGraph()

        def add_nodes(tarefa: Task):
            G.add_node(tarefa.task_name, label=f"{tarefa.task_name} ({tarefa.task_time})")
            for filho in tarefa.children:
                G.add_edge(tarefa.task_name, filho.task_name)
                add_nodes(filho)

        add_nodes(self.raiz)
        pos = nx.nx_pydot.graphviz_layout(G, prog="dot")
        labels = nx.get_node_attributes(G, "label")

        plt.figure(figsize=(10, 6))
        nx.draw(G, pos, with_labels=True, labels=labels,
                node_size=3000, node_color="lightblue",
                font_size=10, font_weight="bold",
                arrows=True, arrowstyle="-|>", arrowsize=15)
        plt.title("Árvore de Tarefas")
        plt.show()


if __name__ == "__main__":
    scheduler = TaskScheduler("/Processamento-em-Nuvem-Alest-2/casos-t1-252/caso-enunciado.txt")
    scheduler.mostrar_arvore()
    tempo, ordem = scheduler.escalonar()
    print(scheduler.to_dict(tempo))

