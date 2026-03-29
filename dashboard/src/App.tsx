import { createSignal, type Component } from 'solid-js';
import Card from './card';

function ops(to_delete: boolean, ip: string, key: string, value?: string) {

}

const App: Component = () => {
  const [ip, setIp] = createSignal("10.150.3.2");
  const [key, setKey] = createSignal("");
  const [value, setValue] = createSignal("");

  const handleOp = (toDelete: boolean) => {
    if (!key()) return alert("Key is required");

    fetch(`http://${ip()}/keys/${key()}`, {
      method: toDelete ? 'DELETE' : 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: value(),
    }).then(() => {
      setKey("");
      setValue("");
    })
  };

  return (
    <div class='relative h-screen max-w-screen w-full overflow-hidden flex p-4 gap-4 bg-zinc-200'>
      <Card ip="10.150.3.2" />
      <Card ip="10.150.3.3" />
      <Card ip="10.150.3.4" />
      <Card ip="10.150.3.5" />
      <Card ip="10.150.3.6" />

      <button command="show-modal" commandfor="db-op" class='absolute right-16 bottom-16 h-16 w-16 rounded-full bg-black text-3xl text-zinc-200 flex items-center justify-center'> + </button>
      <dialog id="db-op" class='self-center justify-self-center'>
        <div class='bg-white w-96 relative flex flex-col p-8 rounded-2xl'>
          <label for="node_ip" class='text-lg font-bold mb-2'>Node IP:</label>
          <select id="node_ip" name="node_ip" class='border-2 border-zinc-500 rounded-md px-2 py-2 mb-4' 
          value={ip()} onInput={(e) => setIp(e.currentTarget.value)}>
            <option selected value="10.150.3.2">10.150.3.2</option>
            <option value="10.150.3.3">10.150.3.3</option>
            <option value="10.150.3.4">10.150.3.4</option>
            <option value="10.150.3.5">10.150.3.5</option>
            <option value="10.150.3.6">10.150.3.6</option>
          </select>
          <label for="key" class='text-lg font-bold mb-2'>Key:</label>
          <input id="key" class='border-2 border-zinc-500 rounded-md px-2 py-1 mb-4' 
          value={key()} onInput={(e) => setKey(e.currentTarget.value)} />
          <label for="value" class='text-lg font-bold mb-2'>Value:</label>
          <input id="value" class='border-2 border-zinc-500 rounded-md px-2 py-1 mb-4' 
          value={value()} onInput={(e) => setValue(e.currentTarget.value)} />
          <div class='flex gap-4 mt-4'>
            <button class=' w-full border-red-600 hover:bg-red-600 hover:text-black text-red-600 px-4 py-2 border-2 rounded-md' onClick={() => handleOp(true)}>DELETE</button>
            <button class=' w-full border-green-600 hover:bg-green-600 hover:text-black text-green-600 px-4 py-2 border-2 rounded-md' onClick={() => handleOp(false)}>SET</button>
          </div>
          <button command="close" commandfor="db-op" class="absolute right-1.5 top-1.5 border-2 border-red-600 text-red-500 hover:bg-red-500 hover:text-white px-2 py-0.5">X</button>
        </div>
      </dialog>
    </div>
  );
};

export default App;
