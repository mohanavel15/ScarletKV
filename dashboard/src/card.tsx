import { createSignal, For, onMount } from "solid-js";
import { createStore } from "solid-js/store";

type CardProps = {
    ip: string
}

async function fetchData(ip: string): Promise<State | string> {
    try {
        const controller = new AbortController()
        const timeoutId = setTimeout(() => controller.abort(), 250);
        const response = await fetch(`http://${ip}/status`, { signal: controller.signal });
        clearTimeout(timeoutId)
        const output: State = await response.json();
        return output
    } catch (error) {
        console.log(`Error fetching data for IP ${ip}:`, error);
        return 'Error fetching data';
    }
}


async function killNode(ip: string): Promise<void> {
    try {
        await fetch(`http://${ip}/kill`);
    } catch (error) {
        console.log(`Error killing node at IP ${ip}:`, error);
    }
}
// {"term":1,"log_index":-1,"state":0,"voted_for":"","commit_index":-1,"logs":[],"store":{},"timeout":272}

type State = {
    term: number,
    log_index: number,
    state: number,
    voted_for: string,
    commit_index: number,
    logs: any[],
    store: Record<string, string>,
    timeout: number
}

function getStateString(state: number): string {
    switch (state) {
        case 0:
            return 'Follower';
        case 1:
            return 'Candidate';
        case 2:
            return 'Leader';
        default:
            return 'Unknown';
    }
}

export default function Card(props: CardProps) {
    const [isOffline, setIsOffline] = createSignal(true);
    const [state, setState] = createStore<State>({ term : -1, log_index: -1, state: -1, voted_for: '', commit_index: -1, logs: [], store: {}, timeout: 0 });

    onMount(() => {
        setInterval(async () => {
            const status_str = await fetchData(props.ip);
            if (typeof status_str === 'string') {
                setIsOffline(true);
                return;
            }
            setIsOffline(false);
            setState(status_str as State);
        }, 500);
    });

  return (
    <div class={`h-full w-full border-2 ${ isOffline() ? 'bg-red-300' : (state.state == 2 ? 'bg-amber-200' : 'bg-green-300') } border-black p-4`}>
        <div class="flex w-full justify-between">
            <h2 class='text-lg font-bold mb-2'>IP: {props.ip}</h2>
            <button class="border-red-600 hover:bg-red-600 hover:text-black text-red-600 px-1.5 py-0.5 border-2 rounded-md" onClick={() => killNode(props.ip)}>kill</button>
            </div>
        <p>Term: {state.term}</p>
        <p>Log Index: {state.log_index}</p>
        <p>State: {getStateString(state.state)}</p>
        <p>Voted For: {state.voted_for}</p>
        <p>Commit Index: {state.commit_index}</p>
        <div>
            <p>Logs:</p>
            <For each={state.logs} fallback={<div>N/A</div>}>
                {(item) => <div>{item}</div>}
            </For>
        </div>
        <div>
            <p>Store:</p>
            <table class="table-auto border-collapse border border-zinc-500 w-full">
                <thead>
                    <tr>
                        <th class="border border-zinc-500 px-4 py-2">Key</th>
                        <th class="border border-zinc-500 px-4 py-2">Value</th>
                    </tr>
                </thead>
                <tbody>
                    <For each={Object.entries(state.store)} fallback={<tr>
                                <td class="border border-zinc-500 px-4 py-2">N/A</td>
                                <td class="border border-zinc-500 px-4 py-2">N/A</td>
                            </tr>}>
                        {([key, value]) => (
                            <tr id={key}>
                                <td class="border border-zinc-500 px-4 py-2">{key}</td>
                                <td class="border border-zinc-500 px-4 py-2">{value}</td>
                            </tr>
                        )}
                    </For>
                </tbody>
            </table>
        </div>
        <p>Timeout: {state.timeout}ms</p>
    </div>
  );
};
